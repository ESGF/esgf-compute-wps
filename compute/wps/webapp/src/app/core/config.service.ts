import { Injectable } from '@angular/core';

@Injectable()
export class ConfigService {
  esgfURL = 'https://esgf.llnl.gov';
  cogURL = 'https://esgf-node.llnl.gov/search/esgf-llnl';
}
