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
  loginPath = `${this.basePath}/auth/login/openid`;
  logoutPath = `${this.basePath}/auth/logout`;
  profilePath = `${this.basePath}/user/profile`;
  userJobPath = `${this.basePath}/user/jobs`;
  configurePath = `${this.basePath}/configure`;

  apiBasePath = 'https://10.5.5.5';

  // WPS API paths
  wpsPath = `${this.apiBasePath}/wps/`;
  jobsPath = `${this.apiBasePath}/wps/jobs/`;
  generatePath = `${this.apiBasePath}/wps/generate/`;
  searchPath = `${this.apiBasePath}/wps/search/`;
  searchVariablePath = `${this.apiBasePath}/wps/search/variable/`;
  combinePath = `${this.apiBasePath}/wps/combine`;
 
  // Auth API paths
  authLoginOpenIDPath = `${this.apiBasePath}/auth/login/openid/`;
  authLoginMPCPath = `${this.apiBasePath}/auth/login/mpc/`;
  authLoginOAuth2Path = `${this.apiBasePath}/auth/login/oauth2/`;
  authLogoutPath = `${this.apiBasePath}/auth/logout/`;
  authUpdatePath = `${this.apiBasePath}/auth/update/`;
  authUserPath = `${this.apiBasePath}/auth/user/`;
  authUserCertPath = `${this.apiBasePath}/auth/user/cert/`;
  authUserRegenPath = `${this.apiBasePath}/auth/user/regenerate/`;
  authUserStatsPath = `${this.apiBasePath}/auth/user/stats/`;
}
