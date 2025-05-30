/**
 * Generated by orval v7.9.0 🍺
 * Do not edit manually.
 * UI Router API
 * Defines the specification for the UI Catalog API
 * OpenAPI spec version: 1.0.2
 */
import type { OrderDirection } from './orderDirection';

export type GetVolumesParams = {
  /**
   * Volumes offset
   */
  offset?: number;
  /**
   * Volumes limit
   */
  limit?: number;
  /**
   * Volumes search
   */
  search?: string;
  /**
   * Order by: volume_name (default), volume_type, created_at, updated_at
   */
  order_by?: string;
  /**
   * Order direction: ASC, DESC (default)
   */
  order_direction?: OrderDirection;
};
