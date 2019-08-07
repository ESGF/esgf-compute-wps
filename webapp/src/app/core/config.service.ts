import { Injectable } from '@angular/core';

@Injectable()
export class ConfigService {
  certEnabled = true;
  workflowEnabled = false;

  esgfURL = 'https://esgf.llnl.gov';
  cogURL = 'https://esgf-node.llnl.gov/search/esgf-llnl';
  apiURL = 'https://github.com/ESGF/esgf-compute-api';
  basePath = '/';
  //basePath = '/wps/home';
  
  // WebApp paths
  loginPath = `${this.basePath}/auth/login`;
  logoutPath = `${this.basePath}/auth/logout`;
  profilePath = `${this.basePath}/user/profile`;
  userJobPath = `${this.basePath}/user/jobs`;
  configurePath = `${this.basePath}/configure`;

  serverPath = '';

  // WPS entrypoint
  wpsPath = `${this.serverPath}/wps/`;

  // API base path
  apiBasePath = `${this.serverPath}/api`;

  // WPS API paths
  jobsPath = `${this.apiBasePath}/jobs/`;
  generatePath = `${this.apiBasePath}/generate/`;
  searchPath = `${this.apiBasePath}/search/`;
  searchVariablePath = `${this.apiBasePath}/search/variable/`;
  combinePath = `${this.apiBasePath}/combine/`;
  providerPath = `${this.apiBasePath}/providers`;
 
  // Auth API paths
  authLoginOpenIDPath = `${this.apiBasePath}/openid/login/`;
  authLoginMPCPath = `${this.apiBasePath}/mpc/`;
  authLoginOAuth2Path = `${this.apiBasePath}/oauth2/`;
  authLogoutPath = `${this.apiBasePath}/openid/logout/`;
  authUpdatePath = `${this.apiBasePath}/user/update/`;
  authUserPath = `${this.apiBasePath}/user/`;
  authUserCertPath = `${this.apiBasePath}/user/cert/`;
  authUserRegenPath = `${this.apiBasePath}/user/regenerate/`;
  authUserStatsPath = `${this.apiBasePath}/user/stats/`;
}
