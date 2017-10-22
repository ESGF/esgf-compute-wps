import { Component, OnInit } from '@angular/core';
import { ActivatedRoute, Router, NavigationExtras } from '@angular/router';

import { AuthService, User } from './auth.service';
import { NotificationService } from './notification.service';

@Component({ 
  template: '',
})
export class LoginCallbackComponent implements OnInit {
  constructor(
    private authService: AuthService,
    private notificationService: NotificationService,
    private router: Router
  ) { }

  ngOnInit() {
    this.authService.isLoggedIn$.subscribe((value: boolean) => {
      if (value) {
        this.router.navigate(['/wps/home/profile']);

        this.notificationService.message('Successfully authenticated to ESGF OpenID');
      }
    });
  }
}

@Component({
  templateUrl: './login.component.html',
  styleUrls: ['./forms.css']
})
export class LoginComponent implements OnInit {
  model: User = {} as User;
  next: string;

  constructor(
    private authService: AuthService,
    private notificationService: NotificationService,
    private route: ActivatedRoute,
    private router: Router
  ) { }

  ngOnInit() { 
    this.authService.isLoggedIn$.subscribe((value: boolean) => {
      if (value) {
        let redirect = this.authService.redirectUrl || '/wps/home/profile';

        let navigationExtras: NavigationExtras = {
          queryParamsHandling: 'preserve',
          preserveFragment: true
        };

        this.router.navigate([redirect], navigationExtras);
      }
    });
  }

  onSubmit() {
    this.authService.login(this.model);
  }
}
