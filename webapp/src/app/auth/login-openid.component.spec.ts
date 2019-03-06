import { DebugElement } from '@angular/core';
import { TestBed, fakeAsync, tick, ComponentFixture } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';
import { Http } from '@angular/http';

import { AuthService } from '../core/auth.service';
import { NotificationService } from '../core/notification.service';
import { LoginOpenIDComponent } from './login-openid.component';

describe('Forgot Password Component', () => {
  let fixture: ComponentFixture<LoginOpenIDComponent>;
  let comp: LoginOpenIDComponent;

  let auth: AuthService;
  let notification: NotificationService;
  let messageSpy: any;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [FormsModule],
      declarations: [LoginOpenIDComponent],
      providers: [
        {provide: Http, useValue: jasmine.createSpy('http')},
        AuthService,
        NotificationService,
      ],
    });

    fixture = TestBed.createComponent(LoginOpenIDComponent);

    comp = fixture.componentInstance;

    auth = fixture.debugElement.injector.get(AuthService);

    notification = fixture.debugElement.injector.get(NotificationService);

    spyOn(notification, 'error');

    spyOn(notification, 'message');
  });

  it('should emit a message notification', fakeAsync(() => {
    let redirect = 'https://test.com/redirect';

    spyOn(auth, 'loginOpenID').and.returnValue(Promise.resolve({ status: 'success', data: { redirect: redirect } }));

    spyOn(comp, 'redirect');

    comp.onSubmit();

    fixture.detectChanges();

    tick();

    fixture.detectChanges();

    expect(comp.redirect).toHaveBeenCalled();
    expect(comp.redirect).toHaveBeenCalledWith(redirect);
  }));

  it('should emit an error notification', fakeAsync(() => {
    spyOn(auth, 'loginOpenID').and.returnValue(Promise.reject({ status: 'failed' }));

    comp.onSubmit();

    fixture.detectChanges();

    tick();

    fixture.detectChanges();

    expect(notification.error).toHaveBeenCalled();
  }));
});
