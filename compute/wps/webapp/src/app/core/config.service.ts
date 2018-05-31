import { Injectable } from '@angular/core';

@Injectable()
export class ConfigService {
  esgfURL = 'https://esgf.llnl.gov';
  cogURL = 'https://esgf-node.llnl.gov/search/esgf-llnl';
  basePath = '/wps/home';
  
  // WebApp paths
  loginPath = `${this.basePath}/auth/login/openid`;
  logoutPath = `${this.basePath}/auth/logout`;
  profilePath = `${this.basePath}/user/profile`;
  userJobPath = `${this.basePath}/user/jobs`;
  configurePath = `${this.basePath}/configure`;
  adminPath = `${this.basePath}/admin`;

  // WPS API paths
  wpsPath = '/wps';
  jobsPath = '/wps/jobs/';
  generatePath = '/wps/generate/';
  processesPath = '/wps/processes/';
  searchPath = '/wps/search/';
  searchVariablePath = '/wps/search/variable/';
  adminStatsPath = '/wps/admin/stats/';
 
  // Auth API paths
  authLoginPath = '/auth/login/';
  authLoginOpenIDPath = '/auth/login/openid/';
  authLoginMPCPath = '/auth/login/mpc/';
  authLoginOAuth2Path = '/auth/login/oauth2/';
  authLogoutPath = '/auth/logout/';
  authResetPath = '/auth/reset/';
  authForgotPasswordPath = '/auth/forgot/password/';
  authForgotUsernamePath = '/auth/forgot/username/';
  authCreatePath = '/auth/create/';
  authUpdatePath = '/auth/update/';
  authUserPath = '/auth/user/';
  authUserRegenPath = '/auth/user/regenerate/';
  authUserStatsPath = '/auth/user/stats/';
}
