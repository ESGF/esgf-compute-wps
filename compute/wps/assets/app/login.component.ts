import { Component, OnInit } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';

import { User } from './user';
import { AuthService } from './auth.service';

@Component({ 
  template: '',
})
export class LoginCallbackComponent implements OnInit {
  constructor(
    private authService: AuthService,
    private route: ActivatedRoute,
    private router: Router
  ) { }

  ngOnInit() {
    this.authService.setExpires(this.route.snapshot.queryParams.expires);

    this.router.navigate(['/wps/home/profile']);
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
    private route: ActivatedRoute,
    private router: Router
  ) { }

  ngOnInit() {
    this.next = this.route.snapshot.queryParams['next'] || '/wps/home/profile';
  }

  onSubmit() {
    this.authService.login(this.model)
      .then(response => this.router.navigateByUrl(this.next))
      .catch(error => console.log(error));
  }
}
