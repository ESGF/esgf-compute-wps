import { Component } from '@angular/core';

import { AuthService } from './auth.service';
import { NotificationService } from './notification.service';

class Provider {
  constructor(
    public name: string, 
    public url: string
  ) { }
}

@Component({
  templateUrl: './login-openid.component.html',
  styleUrls: ['./forms.css']
})
export class LoginOpenIDComponent {
  PROVIDERS: Array<Provider> = [
    new Provider( 
      'DOE Lawrence Livermore National Laboratory (LLNL)',
      'https://esgf-node.llnl.gov/esgf-idp/openid/'
    ),
    new Provider( 
      'Centre for Environmental Data Analysis (CEDA)',
      'https://ceda.ac.uk/openid/'
    ),
    new Provider( 
      'NASA Jet Propulsion Laboratory (JPL)',
      'https://esgf-node.jpl.nasa.gov/esgf-idp/openid/'
    ),
    new Provider( 
      'Institut Pierre Simon Laplace (IPSL)',
      'https://esgf-node.ipsl.upmc.fr/esgf-idp/openid/'
    ),
    new Provider( 
      'National Supercomputer Center at Linkoping University (NSC-LIU)',
      'https://esg-dn1.nsc.liu.se/esgf-idp/openid/'
    ),
    new Provider( 
      'German Climate Computing Centre (DKRZ)',
      'https://esgf-data.dkrz.de/esgf-idp/openid/'
    ), 
    new Provider( 
      'NASA Center for Climate Simulation (NCCS)',
      'https://esgf.nccs.nasa.gov/esgf-idp/openid/'
    ),
    new Provider( 
      'National Computational Infrastructure (NCI)',
      'https://esgf.nci.org.au/esgf-idp/openid/'
    ),
    new Provider( 
      'NOAA Geophysical Fluid Dynamics Laboratory (GFDL)',
      'https://esgdata.gfdl.noaa.gov/esgf-idp/openid/'
    ),
    new Provider( 
      'NOAA Enviromental System Research Laboratory (ESRL)',
      'https://esgf.esrl.noaa.gov/esgf-idp/openid/'
    )
  ];

  model: any = {
    idp: this.PROVIDERS[0]
  };

  constructor(
    private authService: AuthService,
    private notificationService: NotificationService,
  ) { }

  onLogin() {
    this.authService.loginOpenID(this.model.idp.url)
      .then(response => this.handleLogin(response))
      .catch(error => console.log(error));
  }

  handleLogin(response: any) {
    if (response.status === 'success') {
      window.location.replace(response.data.redirect);
    } else if (response.status === 'failed') {
      this.notificationService.error(response.error);
    }
  }
}
