import os
from pathlib import Path;

from typing import List, Optional

from slt_runner.logger import logger
from slt_runner.my_token import Token, TokenType

from slt_runner.expected_result import ExpectedResult

from slt_runner.statement import (
    Statement,
    Mode,
    Halt,
    Set,
    Load,
    Query,
    HashThreshold,
    Loop,
    Foreach,
    Endloop,
    Restart,
    Reconnect,
    Sleep,
    Skip,
    Unskip,
    SortStyle,
    Stub,
)
from slt_runner.statement.sleep import get_sleep_unit, SleepUnit

from slt_runner.decorator import SkipIf, OnlyIf, ExcludeFromCoverage

from slt_runner.base_decorator import BaseDecorator
from slt_runner.base_statement import BaseStatement
from slt_runner.test import SQLLogicTest


def create_formatted_list(items) -> str:
    res = ''
    for i, option in enumerate(items):
        if i + 1 == len(items):
            spacer = ' or '
        elif i != 0:
            spacer = ', '
        else:
            spacer = ''
        res += f"{spacer}'{option}'"
    return res


def is_space(char: str):
    return char == ' ' or char == '\t' or char == '\n' or char == '\v' or char == '\f' or char == '\r'


### -------- PARSER ----------
class SQLParserException(Exception):
    def __init__(self, message):
        self.message = "Parser Error: " + message
        super().__init__(self.message)


class SQLLogicParser:
    def reset(self, derive_from):
        self.current_line = 0
        self.seen_statement = False
        self.lines = []
        self.current_test = None
        self.derive_from = derive_from

    def __init__(self, derive_from):
        self.reset(derive_from)
        self.STATEMENTS = {
            TokenType.SQLLOGIC_STATEMENT: self.statement_statement,
            TokenType.SQLLOGIC_QUERY: self.statement_query,
            TokenType.SQLLOGIC_REQUIRE: self.statement_require,
            TokenType.SQLLOGIC_HASH_THRESHOLD: self.statement_hash_threshold,
            TokenType.SQLLOGIC_HALT: self.statement_halt,
            TokenType.SQLLOGIC_MODE: self.statement_mode,
            TokenType.SQLLOGIC_SET: self.statement_set,
            TokenType.SQLLOGIC_LOOP: self.statement_loop,
            TokenType.SQLLOGIC_CONCURRENT_LOOP: self.statement_loop,
            TokenType.SQLLOGIC_FOREACH: self.statement_foreach,
            TokenType.SQLLOGIC_CONCURRENT_FOREACH: self.statement_foreach,
            TokenType.SQLLOGIC_ENDLOOP: self.statement_endloop,
            TokenType.SQLLOGIC_REQUIRE_ENV: self.statement_require_env,
            TokenType.SQLLOGIC_LOAD: self.statement_load,
            TokenType.SQLLOGIC_RESTART: self.statement_restart,
            TokenType.SQLLOGIC_RECONNECT: self.statement_reconnect,
            TokenType.SQLLOGIC_SLEEP: self.statement_sleep,
            TokenType.SQLLOGIC_INVALID: None,
        }
        self.DECORATORS = {
            TokenType.SQLLOGIC_SKIP_IF: self.decorator_skipif,
            TokenType.SQLLOGIC_ONLY_IF: self.decorator_onlyif,
            TokenType.SQLLOGIC_EXCLUDE_FROM_COVERAGE: self.decorator_excludefromcoverage,

        }
        self.FOREACH_COLLECTIONS = {
            # TODO: set snowflake's types.
            "<compression>": ["none", "uncompressed", "rle", "bitpacking", "dictionary", "fsst", "alp", "alprd"],
            "<alltypes>": ["bool", "interval", "varchar"],
            "<numeric>": ["number", "decimal", "float", "real"], # SF OK
            "<integral>": ["tinyint", "smallint", "integer", "bigint", "hugeint"],
            "<signed>": ["tinyint", "smallint", "integer", "bigint", "hugeint"],
            "<unsigned>": ["utinyint", "usmallint", "uinteger", "ubigint", "uhugeint"],
            "<unsigned>": ["utinyint", "usmallint", "uinteger", "ubigint", "uhugeint"],
            "<all_types_columns>": [
                "bool",
                "tinyint",
                "smallint",
                "int",
                "bigint",
                "hugeint",
                "uhugeint",
                "utinyint",
                "usmallint",
                "uint",
                "ubigint",
                "date",
                "time",
                "timestamp",
                "timestamp_s",
                "timestamp_ms",
                "timestamp_ns",
                "time_tz",
                "timestamp_tz",
                "float",
                "double",
                "dec_4_1",
                "dec_9_4",
                "dec_18_6",
                "dec38_10",
                "uuid",
                "interval",
                "varchar",
                "blob",
                "bit",
                "small_enum",
                "medium_enum",
                "large_enum",
                "int_array",
                "double_array",
                "date_array",
                "timestamp_array",
                "timestamptz_array",
                "varchar_array",
                "nested_int_array",
                "struct",
                "struct_of_arrays",
                "array_of_structs",
                "map",
                "union",
                "fixed_int_array",
                "fixed_varchar_array",
                "fixed_nested_int_array",
                "fixed_nested_varchar_array",
                "fixed_struct_array",
                "struct_of_fixed_array",
                "fixed_array_of_int_list",
                "list_of_fixed_int_array",
            ],
        }

    def peek(self):
        return self.peek_no_strip().strip()

    def peek_no_strip(self):
        if self.current_line >= len(self.lines):
            raise SQLParserException("File already fully consumed")
        return self.lines[self.current_line]

    def consume(self):
        if self.current_line >= len(self.lines):
            raise SQLParserException("File already fully consumed")
        self.current_line += 1

    def fail(self, message):
        file_path = self.current_test.path
        error_message = f"{file_path}:{self.current_line + 1}: {message}"
        raise SQLParserException(error_message)

    def get_expected_result(self, statement_type: str) -> ExpectedResult:
        type_map = {
            'ok': ExpectedResult.Type.SUCCESS,
            'error': ExpectedResult.Type.ERROR,
            'maybe': ExpectedResult.Type.UNKNOWN,
        }
        if statement_type not in type_map:
            error = 'statement argument should be ' + create_formatted_list(type_map.keys())
            self.fail(error)
        return ExpectedResult(type_map[statement_type])

    def extract_expected_lines(self) -> Optional[List[str]]:
        end_of_file = self.current_line >= len(self.lines)
        if end_of_file or self.peek() != "----":
            return None

        self.consume()
        result = []
        while self.current_line < len(self.lines) and self.peek_no_strip().strip('\n'):
            result.append(self.peek_no_strip().strip('\n'))
            self.consume()
        return result

    def statement_statement(self, header: Token) -> Optional[BaseStatement]:
        options = ['ok', 'error', 'maybe']
        if len(header.parameters) < 1:
            self.fail(f"statement requires at least one parameter ({create_formatted_list(options)})")
        expected_result = self.get_expected_result(header.parameters[0])

        statement = Statement(header, self.current_line + 1)
        statement.file_name = self.current_test.path

        self.next_line()
        statement_text = self.extract_statement()
        if statement_text == []:
            self.fail("Unexpected empty statement text")
        statement.add_lines(statement_text)
        statement.set_derivative_query(self.derive_from)

        expected_lines: Optional[List[str]] = self.extract_expected_lines()
        if expected_result.type == ExpectedResult.Type.SUCCESS:
            if expected_lines != None:
                if len(expected_lines) != 0:
                    self.fail(
                        "Failed to parse statement: only statement error can have an expected error message, not statement ok"
                    )
                expected_result.add_lines(expected_lines)
        elif expected_result.type == ExpectedResult.Type.ERROR or expected_result.type == ExpectedResult.Type.UNKNOWN:
            if expected_lines != None:
                expected_result.add_lines(expected_lines)
            elif not self.current_test.is_sqlite_test():
                # print(statement)
                self.fail('Failed to parse statement: statement error needs to have an expected error message')
        else:
            self.fail(f"Unexpected ExpectedResult Type: {expected_result.type.name}")

        statement.expected_result = expected_result
        if len(header.parameters) >= 2:
            statement.set_connection(header.parameters[1])
        return statement

    def statement_query(self, header: Token) -> BaseStatement:
        if len(header.parameters) < 1:
            self.fail("query requires at least one parameter (query III)")
        query = Query(header, self.current_line + 1)

        # parse the expected column count
        query.expected_column_count = 0
        column_text = header.parameters[0]
        accepted_chars = ['T', 'I', 'R']
        if not all(x in accepted_chars for x in column_text):
            self.fail(f"Found unknown character in {column_text}, expected {create_formatted_list(accepted_chars)}")
        expected_column_count = len(column_text)

        query.expected_column_count = expected_column_count
        if query.expected_column_count == 0:
            self.fail("Query requires at least a single column in the result")

        query.file_name = self.current_test.path
        query.query_line = self.current_line + 1
        # extract the SQL statement
        self.next_line()
        statement_text = self.extract_statement()
        query.add_lines(statement_text)
        query.set_derivative_query(self.derive_from)

        # extract the expected result
        expected_result = self.get_expected_result('ok')
        expected_lines: Optional[List[str]] = self.extract_expected_lines()
        if expected_lines != None:
            expected_result.add_lines(expected_lines)
        expected_result.set_expected_column_count(expected_column_count)
        query.expected_result = expected_result

        def get_sort_style(parameters: List[str]) -> SortStyle:
            sort_style = SortStyle.NO_SORT
            if len(parameters) > 1:
                sort_style = parameters[1]
                if sort_style == "nosort":
                    # Do no sorting
                    sort_style = SortStyle.NO_SORT
                elif sort_style == "rowsort" or sort_style == "sort":
                    # Row-oriented sorting
                    sort_style = SortStyle.ROW_SORT
                elif sort_style == "valuesort":
                    # Sort all values independently
                    sort_style = SortStyle.VALUE_SORT
                else:
                    sort_style = SortStyle.UNKNOWN
            return sort_style

        # figure out the sort style
        sort_style = get_sort_style(header.parameters)
        if sort_style == SortStyle.UNKNOWN:
            sort_style = SortStyle.NO_SORT
            query.set_connection(header.parameters[1])
        query.set_sortstyle(sort_style)

        # check the label of the query
        if len(header.parameters) > 2:
            query.set_label(header.parameters[2])
        return query

    def statement_hash_threshold(self, header: Token) -> Optional[BaseStatement]:
        if len(header.parameters) != 1:
            self.fail("hash-threshold requires a parameter")
        threshold = int(header.parameters[0])
        return HashThreshold(header, self.current_line + 1, threshold)

    def statement_halt(self, header: Token) -> Optional[BaseStatement]:
        return Halt(header, self.current_line + 1)

    def statement_mode(self, header: Token) -> Optional[BaseStatement]:
        if len(header.parameters) != 1:
            self.fail("mode requires one parameter")
        parameter = header.parameters[0]
        if parameter == "skip":
            return Skip(header, self.current_line + 1)
        elif parameter == "unskip":
            return Unskip(header, self.current_line + 1)
        else:
            return Mode(header, self.current_line + 1, parameter)

    def statement_require(self, header: Token) -> Optional[BaseStatement]:
        return Stub(header, self.current_line + 1)

    def statement_set(self, header: Token) -> Optional[BaseStatement]:
        parameters = header.parameters
        if len(parameters) < 1:
            self.fail("set requires at least 1 parameter (e.g. set ignore_error_messages HTTP Error)")
        accepted_options = ['ignore_error_messages', 'always_fail_error_messages']
        if parameters[0] in accepted_options:
            error_messages = []
            # Parse the parameter list as a comma separated list of strings that can contain spaces
            # e.g. `set ignore_error_messages This is an error message, This_is_another, and   another`
            tmp = [[y.strip() for y in x.split(',') if y.strip() != ''] for x in parameters[1:]]
            for x in tmp:
                error_messages.extend(x)
            statement = Set(header, self.current_line + 1)
            statement.add_error_messages(error_messages)
            return statement
        else:
            self.fail(
                f"unrecognized set parameter: {parameters[0]}, expected {create_formatted_list(accepted_options)}"
            )

    def statement_load(self, header: Token) -> Optional[BaseStatement]:
        statement = Load(header, self.current_line + 1)
        if len(header.parameters) > 1 and header.parameters[1] == "readonly":
            statement.set_readonly()
        return statement

    def statement_loop(self, header: Token) -> Optional[BaseStatement]:
        if len(header.parameters) != 3:
            self.fail("Expected loop [iterator_name] [start] [end] (e.g. loop i 1 300)")
        is_parallel = header.type == TokenType.SQLLOGIC_CONCURRENT_LOOP
        statement = Loop(header, self.current_line + 1, is_parallel)
        statement.set_name(header.parameters[0])
        statement.set_start(int(header.parameters[1]))
        statement.set_end(int(header.parameters[2]))
        return statement

    def statement_foreach(self, header: Token) -> Optional[BaseStatement]:
        if len(header.parameters) < 2:
            self.fail(
                "Expected foreach [iterator_name] [m1] [m2] [etc...] (e.g. foreach type integer " "smallint float)"
            )
        is_parallel = header.type == TokenType.SQLLOGIC_CONCURRENT_FOREACH
        statement = Foreach(header, self.current_line + 1, is_parallel)
        statement.set_name(header.parameters[0])
        raw_values = header.parameters[1:]

        def add_tokens(result, param):
            token_name = param.lower().strip()

            if token_name in self.FOREACH_COLLECTIONS:
                result.extend(self.FOREACH_COLLECTIONS[token_name])
            else:
                result.append(param)

        foreach_tokens = []
        for value in raw_values:
            add_tokens(foreach_tokens, value)

        statement.set_values(foreach_tokens)
        return statement

    def statement_endloop(self, header: Token) -> Optional[BaseStatement]:
        return Endloop(header, self.current_line + 1)

    def statement_require_env(self, header: Token) -> Optional[BaseStatement]:
        return Skip()

    def statement_restart(self, header: Token) -> Optional[BaseStatement]:
        return Restart(header, self.current_line + 1)

    def statement_reconnect(self, header: Token) -> Optional[BaseStatement]:
        return Reconnect(header, self.current_line + 1)

    def statement_sleep(self, header: Token) -> Optional[BaseStatement]:
        if len(header.parameters) != 2:
            self.fail("sleep requires two parameter (e.g. sleep 1 second)")
        sleep_duration = int(header.parameters[0])
        sleep_unit = get_sleep_unit(header.parameters[1])
        if sleep_unit == SleepUnit.UNKNOWN:
            options = ['second', 'millisecond', 'microsecond', 'nanosecond']
            raise self.fail(f"Unrecognized sleep mode - expected {create_formatted_list(options)}")
        return Sleep(header, self.current_line + 1, sleep_duration, sleep_unit)

    # Decorators

    def decorator_skipif(self, token: Token) -> Optional[BaseDecorator]:
        return SkipIf(token)

    def decorator_onlyif(self, token: Token) -> Optional[BaseDecorator]:
        return OnlyIf(token)

    def decorator_excludefromcoverage(self, token: Token) -> Optional[BaseDecorator]:
        return ExcludeFromCoverage(token)

    def parse(self, file_path: str) -> Optional[SQLLogicTest]:
        if not self.open_file(file_path):
            raise SQLParserException(f"Could not find {file_path}")

        while self.next_statement():
            token = self.tokenize()

            # throw explicit error on single line statements that are not separated by a comment or newline
            if self.is_single_line_statement(token) and not self.next_line_empty_or_comment():
                self.fail("All test statements need to be separated by an empty line")

            # Parse any number of decorators first
            parse_method = self.DECORATORS.get(token.type)
            decorators: List[BaseDecorator] = []
            while parse_method != None:
                decorator = parse_method(token)
                if not decorator:
                    self.fail(f"Parser did not produce a decorator for {token.type.name}")
                decorators.append(decorator)
                self.next_line()
                token = self.tokenize()
                parse_method = self.DECORATORS.get(token.type)

            # Then parse the statement
            parse_method = self.STATEMENTS.get(token.type)
            if parse_method:
                logger.debug(f'parsed statement: {token.type}, line:{self.current_line}, {token.text}')
                statement = parse_method(token)
            else:
                self.fail(f"Unexpected token type: {token.type.name}")
            if not statement:
                self.fail(f"Parser did not produce a statement for {token.type.name}")
            statement.add_decorators(decorators)
            self.current_test.add_statement(statement)
        return self.current_test

    def open_file(self, path):
        self.reset(self.derive_from)
        self.current_test = SQLLogicTest(path)
        try:
            with open(Path(path), 'r', encoding='utf8') as infile:
                self.lines = [line.replace("\r", "") for line in infile.readlines()]
                return True
        except IOError:
            # TODO: collect error
            return False
        except UnicodeDecodeError:
            # TODO: collect error
            return False

    def empty_or_comment(self, line):
        return not line.strip('\n') or line.startswith("#")

    def next_line_empty_or_comment(self):
        if self.current_line + 1 >= len(self.lines):
            return True
        else:
            return self.empty_or_comment(self.lines[self.current_line + 1])

    def eof(self):
        return self.current_line >= len(self.lines)

    def next_statement(self):
        if self.seen_statement:
            while not self.eof() and not self.empty_or_comment(self.peek()):
                self.consume()
        self.seen_statement = True

        while not self.eof() and self.empty_or_comment(self.peek()):
            self.consume()

        return not self.eof()

    def next_line(self):
        self.consume()

    def extract_statement(self):
        statement = []

        while not self.eof() and not self.empty_or_comment(self.peek_no_strip()):
            line = self.peek_no_strip()
            if line.strip('\n') == "----":
                break
            statement.append(line.strip('\n'))
            self.consume()
        return statement

    def tokenize(self):
        result = Token()
        if self.current_line >= len(self.lines):
            result.type = TokenType.SQLLOGIC_INVALID
            return result

        line = self.peek_no_strip()
        argument_list = line.split()
        argument_list = [x for x in line.strip('\n').split() if not is_space(x)]

        if not argument_list:
            self.fail("Empty line!?")

        result.text = argument_list[0]
        result.type = self.command_to_token(argument_list[0])
        result.parameters.extend(argument_list[1:])
        return result

    def is_single_line_statement(self, token):
        single_line_statements = [
            TokenType.SQLLOGIC_HASH_THRESHOLD,
            TokenType.SQLLOGIC_HALT,
            TokenType.SQLLOGIC_MODE,
            TokenType.SQLLOGIC_SET,
            TokenType.SQLLOGIC_LOOP,
            TokenType.SQLLOGIC_FOREACH,
            TokenType.SQLLOGIC_CONCURRENT_LOOP,
            TokenType.SQLLOGIC_CONCURRENT_FOREACH,
            TokenType.SQLLOGIC_ENDLOOP,
            TokenType.SQLLOGIC_REQUIRE,
            TokenType.SQLLOGIC_REQUIRE_ENV,
            TokenType.SQLLOGIC_LOAD,
            TokenType.SQLLOGIC_RESTART,
            TokenType.SQLLOGIC_RECONNECT,
            TokenType.SQLLOGIC_SLEEP,
        ]

        if token.type in single_line_statements:
            return True
        elif token.type in [
            TokenType.SQLLOGIC_SKIP_IF,
            TokenType.SQLLOGIC_ONLY_IF,
            TokenType.SQLLOGIC_EXCLUDE_FROM_COVERAGE,
            TokenType.SQLLOGIC_INVALID,
            TokenType.SQLLOGIC_STATEMENT,
            TokenType.SQLLOGIC_QUERY,
        ]:
            return False
        else:
            raise RuntimeError("Unknown SQLLogic token found!")

    def command_to_token(self, token):
        token_map = {
            "skipif": TokenType.SQLLOGIC_SKIP_IF,
            "onlyif": TokenType.SQLLOGIC_ONLY_IF,
            "exclude-from-coverage": TokenType.SQLLOGIC_EXCLUDE_FROM_COVERAGE,
            "statement": TokenType.SQLLOGIC_STATEMENT,
            "query": TokenType.SQLLOGIC_QUERY,
            "hash-threshold": TokenType.SQLLOGIC_HASH_THRESHOLD,
            "halt": TokenType.SQLLOGIC_HALT,
            "mode": TokenType.SQLLOGIC_MODE,
            "set": TokenType.SQLLOGIC_SET,
            "loop": TokenType.SQLLOGIC_LOOP,
            "concurrentloop": TokenType.SQLLOGIC_CONCURRENT_LOOP,
            "foreach": TokenType.SQLLOGIC_FOREACH,
            "concurrentforeach": TokenType.SQLLOGIC_CONCURRENT_FOREACH,
            "endloop": TokenType.SQLLOGIC_ENDLOOP,
            "require": TokenType.SQLLOGIC_REQUIRE,
            "require-env": TokenType.SQLLOGIC_REQUIRE_ENV,
            "load": TokenType.SQLLOGIC_LOAD,
            "restart": TokenType.SQLLOGIC_RESTART,
            "reconnect": TokenType.SQLLOGIC_RECONNECT,
            "sleep": TokenType.SQLLOGIC_SLEEP,
        }

        if token in token_map:
            return token_map[token]
        else:
            self.fail(f"Unrecognized parameter {token}")
            return TokenType.SQLLOGIC_INVALID

import argparse


def main():
    parser = argparse.ArgumentParser(description="SQL Logic Parser")
    parser.add_argument("filename", type=str, help="Path to the SQL logic file")
    args = parser.parse_args()

    filename = args.filename

    parser = SQLLogicParser()
    out: Optional[SQLLogicTest] = parser.parse(filename)
    if not out:
        raise SQLParserException(f"Test {filename} could not be parsed")


if __name__ == "__main__":
    main()
