import { DebugElement } from '@angular/core';
import { TestBed, fakeAsync, tick, ComponentFixture } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';
import { Http } from '@angular/http';

import { AuthService } from '../core/auth.service';
import { NotificationService } from '../core/notification.service';
import { ForgotUsernameComponent } from './forgot-username.component';

describe('Forgot Password Component', () => {
  let fixture: ComponentFixture<ForgotUsernameComponent>;
  let comp: ForgotUsernameComponent;

  let auth: AuthService;
  let notification: NotificationService;
  let messageSpy: any;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [FormsModule],
      declarations: [ForgotUsernameComponent],
      providers: [
        {provide: Http, useValue: jasmine.createSpy('http')},
        AuthService,
        NotificationService,
      ],
    });

    fixture = TestBed.createComponent(ForgotUsernameComponent);

    comp = fixture.componentInstance;

    auth = fixture.debugElement.injector.get(AuthService);

    notification = fixture.debugElement.injector.get(NotificationService);

    spyOn(notification, 'error');

    spyOn(notification, 'message');
  });

  it('should emit a message notification', fakeAsync(() => {
    spyOn(auth, 'forgotUsername').and.returnValue(Promise.resolve({ status: 'success', data: { redirect: '' } }));

    spyOn(comp, 'redirect');

    comp.onSubmit();

    fixture.detectChanges();

    tick();

    fixture.detectChanges();

    expect(notification.message).toHaveBeenCalled();
  }));

  it('should emit an error notification', fakeAsync(() => {
    spyOn(auth, 'forgotUsername').and.returnValue(Promise.resolve({ status: 'failed' }));

    comp.onSubmit();

    fixture.detectChanges();

    tick();

    fixture.detectChanges();

    expect(notification.error).toHaveBeenCalled();
  }));
});
