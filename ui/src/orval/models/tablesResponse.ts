/**
 * Generated by orval v7.9.0 🍺
 * Do not edit manually.
 * UI Router API
 * Defines the specification for the UI Catalog API
 * OpenAPI spec version: 1.0.2
 */
import type { Table } from './table';
import type { TablesResponseCurrentCursor } from './tablesResponseCurrentCursor';

export interface TablesResponse {
  currentCursor?: TablesResponseCurrentCursor;
  items: Table[];
  nextCursor: string;
}
