import { AxisMeta } from './axis-meta';

export interface FileMeta {
  spatial: AxisMeta[];
  temporal: AxisMeta;
  url: string;
}
