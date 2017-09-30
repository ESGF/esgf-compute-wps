import { Component, OnInit } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';

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
    this.authService.getUserDetails();

    this.router.navigate(['/wps/home/profile']);

    this.notificationService.message('Successfully authenticated to ESGF OpenID');
  }
}

@Component({
  templateUrl: './login.component.html',
  styleUrls: ['./forms.css']
})
export class LoginComponent implements OnInit {
  model: User = new User();
  next: string;

  constructor(
    private authService: AuthService,
    private notificationService: NotificationService,
    private route: ActivatedRoute,
    private router: Router
  ) { }

  ngOnInit() {
    this.next = this.route.snapshot.queryParams['next'] || '/wps/home/profile';
  }

  onSubmit() {
    this.authService.login(this.model)
      .then(response => {
        if (response.status === 'success') {
          this.router.navigateByUrl(this.next)

          this.notificationService.message('Login success');
        } else {
          this.notificationService.error(`Login failed: "${response.error}"`);
        }
      })
      .catch(error => console.log(error));
  }
}
