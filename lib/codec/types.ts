import { PrimitiveType } from '../declare';
import { ParquetCodec, OriginalType, LogicalType, ParquetField } from '../declare';
import { Statistics } from '../../gen-nodejs/parquet_types';

export interface Options {
  typeLength: number;
  bitWidth: number;
  disableEnvelope?: boolean;
  primitiveType?: PrimitiveType;
  originalType?: OriginalType;
  logicalType?: LogicalType;
  encoding?: ParquetCodec;
  compression?: string;
  column?: ParquetField;
  rawStatistics?: Statistics;
  cache?: unknown;
  dictionary?: number[];
  num_values?: number;
  rLevelMax?: number;
  dLevelMax?: number;
  type?: string;
  name?: string;
  precision?: number;
  scale?: number;
  isAdjustedToUTC?: boolean;
  unit?: 'MILLIS' | 'MICROS' | 'NANOS';
}

export interface Cursor {
  buffer: Buffer;
  offset: number;
  size?: number;
}
