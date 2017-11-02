import { DebugElement } from '@angular/core';
import { TestBed, fakeAsync, tick, ComponentFixture } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { Http } from '@angular/http';
import { BehaviorSubject } from 'rxjs/BehaviorSubject';

import { AuthService } from '../core/auth.service';
import { NotificationService } from '../core/notification.service';
import { LoginCallbackComponent } from './login-callback.component';

describe('Forgot Password Component', () => {
  let fixture: ComponentFixture<LoginCallbackComponent>;
  let comp: LoginCallbackComponent;

  let router: any;
  let auth: AuthService;
  let notification: NotificationService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [FormsModule],
      declarations: [LoginCallbackComponent],
      providers: [
        {provide: Router, useValue: jasmine.createSpyObj('router', ['navigate'])},
        {provide: Http, useValue: jasmine.createSpy('http')},
        AuthService,
        NotificationService,
      ],
    });

    fixture = TestBed.createComponent(LoginCallbackComponent);

    comp = fixture.componentInstance;

    auth = fixture.debugElement.injector.get(AuthService);

    router = fixture.debugElement.injector.get(Router);

    notification = fixture.debugElement.injector.get(NotificationService);

    spyOn(notification, 'message');
  });

  it('should redirect', fakeAsync(() => {
    auth.isLoggedIn$ = new BehaviorSubject<boolean>(true);

    comp.ngOnInit();

    fixture.detectChanges();

    expect(notification.message).toHaveBeenCalled();
    expect(router.navigate).toHaveBeenCalled();
  }));

  it('should not redirect', fakeAsync(() => {
    auth.isLoggedIn$ = new BehaviorSubject<boolean>(false);

    comp.ngOnInit();

    fixture.detectChanges();

    expect(router.navigate).not.toHaveBeenCalled();
  }));
});
