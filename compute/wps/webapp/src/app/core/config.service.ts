import { Injectable } from '@angular/core';

@Injectable()
export class ConfigService {
  esgfURL = 'https://esgf.llnl.gov';
  cogURL = 'https://esgf-node.llnl.gov/search/esgf-llnl';
  basePath = '/wps/home';
  loginPath = this.combinePath(this.basePath, '/auth/login/openid');
  logoutPath = this.combinePath(this.basePath, '/auth/logout');
  profilePath = this.combinePath(this.basePath, '/user/profile');
  userJobPath = this.combinePath(this.basePath, '/user/jobs');
  configurePath = this.combinePath(this.basePath, '/configure');
  adminPath = this.combinePath(this.basePath, '/admin');

  combinePath(base: string, path: string) {
    return `${base}${path}`;
  }
}
