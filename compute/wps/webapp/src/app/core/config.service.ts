import { Injectable } from '@angular/core';

@Injectable()
export class ConfigService {
  certEnabled = true;
  workflowEnabled = false;

  esgfURL = 'https://esgf.llnl.gov';
  cogURL = 'https://esgf-node.llnl.gov/search/esgf-llnl';
  apiURL = 'https://github.com/ESGF/esgf-compute-api';
  basePath = '/wps/home';
  
  // WebApp paths
  loginPath = `${this.basePath}/auth/login/openid`;
  logoutPath = `${this.basePath}/auth/logout`;
  profilePath = `${this.basePath}/user/profile`;
  userJobPath = `${this.basePath}/user/jobs`;
  configurePath = `${this.basePath}/configure`;

  // WPS API paths
  wpsPath = '/wps/';
  jobsPath = '/wps/jobs/';
  generatePath = '/wps/generate/';
  searchPath = '/wps/search/';
  searchVariablePath = '/wps/search/variable/';
 
  // Auth API paths
  authLoginOpenIDPath = '/auth/login/openid/';
  authLoginMPCPath = '/auth/login/mpc/';
  authLoginOAuth2Path = '/auth/login/oauth2/';
  authLogoutPath = '/auth/logout/';
  authUpdatePath = '/auth/update/';
  authUserPath = '/auth/user/';
  authUserCertPath = '/auth/user/cert/';
  authUserRegenPath = '/auth/user/regenerate/';
  authUserStatsPath = '/auth/user/stats/';
}
