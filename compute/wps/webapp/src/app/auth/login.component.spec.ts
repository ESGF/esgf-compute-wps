import { DebugElement } from '@angular/core';
import { By } from '@angular/platform-browser';
import { TestBed, fakeAsync, tick, ComponentFixture } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';
import { Http } from '@angular/http';
import { Router } from '@angular/router';
import { BehaviorSubject } from 'rxjs/BehaviorSubject';

import { AuthService } from '../core/auth.service';
import { NotificationService } from '../core/notification.service';
import { LoginComponent } from './login.component';

describe('Forgot Password Component', () => {
  let fixture: ComponentFixture<LoginComponent>;
  let comp: LoginComponent;

  let router: any;
  let auth: AuthService;
  let notification: NotificationService;

  let username: DebugElement;
  let password: DebugElement;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [FormsModule],
      declarations: [LoginComponent],
      providers: [
        {provide: Http, useValue: jasmine.createSpy('http')},
        {provide: Router, useValue: jasmine.createSpyObj('router', ['navigateByUrl'])},
        AuthService,
        NotificationService,
      ],
    });

    fixture = TestBed.createComponent(LoginComponent);

    comp = fixture.componentInstance;

    auth = fixture.debugElement.injector.get(AuthService);

    router = fixture.debugElement.injector.get(Router);

    notification = fixture.debugElement.injector.get(NotificationService);

    spyOn(notification, 'error');

    spyOn(notification, 'message');

    username = fixture.debugElement.query(By.css('#username'));

    password = fixture.debugElement.query(By.css('#password'));
  });

  it('should login', fakeAsync(() => {
    spyOn(auth, 'login');

    fixture.detectChanges();

    fixture.whenStable().then(() => {
      username.nativeElement.value = 'test';
      username.nativeElement.dispatchEvent(new Event('input'));

      password.nativeElement.value = 'test_password';
      password.nativeElement.dispatchEvent(new Event('input'));

      fixture.detectChanges();

      comp.onSubmit();

      expect(auth.login).toHaveBeenCalledWith('test', 'test_password');
    });
  }));

  it('should redirect with redirectUrl, preserving parameters', fakeAsync(() => {
    auth.isLoggedIn$ = new BehaviorSubject<boolean>(true);

    let redirect = '/wps/home/configure?dataset_id=test';

    auth.redirectUrl = redirect

    comp.ngOnInit();

    fixture.detectChanges();

    expect(router.navigateByUrl).toHaveBeenCalled();
    expect(router.navigateByUrl).toHaveBeenCalledWith(redirect);
  }));

  it('should redirect with redirectUrl', fakeAsync(() => {
    auth.isLoggedIn$ = new BehaviorSubject<boolean>(true);

    let redirect = '/wps/home/configure';

    auth.redirectUrl = redirect

    comp.ngOnInit();

    fixture.detectChanges();

    expect(router.navigateByUrl).toHaveBeenCalled();
    expect(router.navigateByUrl).toHaveBeenCalledWith(redirect);
  }));

  it('should redirect if already logged in', fakeAsync(() => {
    auth.isLoggedIn$ = new BehaviorSubject<boolean>(true);

    comp.ngOnInit();

    fixture.detectChanges();

    expect(router.navigateByUrl).toHaveBeenCalled();
    expect(router.navigateByUrl).toHaveBeenCalledWith('/wps/home/user/profile');
  }));
});
