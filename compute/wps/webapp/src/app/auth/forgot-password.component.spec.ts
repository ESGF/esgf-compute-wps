import { DebugElement } from '@angular/core';
import { TestBed, fakeAsync, tick, ComponentFixture } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';
import { Http } from '@angular/http';

import { AuthService } from '../core/auth.service';
import { NotificationService } from '../core/notification.service';
import { ForgotPasswordComponent } from './forgot-password.component';

describe('Forgot Password Component', () => {
  let fixture: ComponentFixture<ForgotPasswordComponent>;
  let comp: ForgotPasswordComponent;

  let auth: AuthService;
  let notification: NotificationService;
  let messageSpy: any;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [FormsModule],
      declarations: [ForgotPasswordComponent],
      providers: [
        {provide: Http, useValue: jasmine.createSpy('http')},
        AuthService,
        NotificationService,
      ],
    });

    fixture = TestBed.createComponent(ForgotPasswordComponent);

    comp = fixture.componentInstance;

    auth = fixture.debugElement.injector.get(AuthService);

    notification = fixture.debugElement.injector.get(NotificationService);

    spyOn(notification, 'error');

    spyOn(notification, 'message');
  });

  it('should emit a message notification', fakeAsync(() => {
    spyOn(auth, 'forgotPassword').and.returnValue(Promise.resolve({ status: 'success' }));

    comp.onSubmit();

    fixture.detectChanges();

    tick();

    fixture.detectChanges();

    expect(notification.message).toHaveBeenCalled();
  }));

  it('should emit an error notification', fakeAsync(() => {
    spyOn(auth, 'forgotPassword').and.returnValue(Promise.reject({ status: 'failed' }));

    comp.onSubmit();

    fixture.detectChanges();

    tick();

    fixture.detectChanges();

    expect(notification.error).toHaveBeenCalled();
  }));
});
