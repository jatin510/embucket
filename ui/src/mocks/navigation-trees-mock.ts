import type { NavigationTreeDatabase } from '@/orval/models';

export const NAVIGATION_TREES_MOCK: NavigationTreeDatabase[] = [
  {
    name: 'database1',
    schemas: [
      {
        name: 'schema1',
        views: [],
        tables: [
          {
            name: 'table1',
          },
          {
            name: 'table2',
          },
          {
            name: 'table3',
          },
        ],
      },
      {
        name: 'schema2',
        views: [],
        tables: [
          {
            name: 'table4',
          },
          {
            name: 'table5',
          },
          {
            name: 'table6',
          },
        ],
      },
    ],
  },
  {
    name: 'database2',
    schemas: [
      {
        name: 'schema3',
        views: [],
        tables: [
          {
            name: 'table7',
          },
          {
            name: 'table8',
          },
          {
            name: 'table9',
          },
        ],
      },
      {
        name: 'schema4',
        views: [],
        tables: [
          {
            name: 'table10',
          },
          {
            name: 'table11',
          },
          {
            name: 'table12',
          },
        ],
      },
    ],
  },
  {
    name: 'database3',
    schemas: [
      {
        name: 'schema5',
        views: [],
        tables: [
          {
            name: 'table13',
          },
          {
            name: 'table14',
          },
          {
            name: 'table15',
          },
        ],
      },
      {
        name: 'schema6',
        views: [],
        tables: [
          {
            name: 'table16',
          },
          {
            name: 'table17',
          },
          {
            name: 'table18',
          },
        ],
      },
    ],
  },
];
