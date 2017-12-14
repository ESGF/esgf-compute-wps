import { Injectable } from '@angular/core';

@Injectable()
export class ConfigService {
  esgfURL: string = 'https://esgf.llnl.gov';
  cogURL: string = 'http://10.5.5.5:8010/search/testproject';
}
