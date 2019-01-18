import { DebugElement } from '@angular/core';
import { By } from '@angular/platform-browser';
import { TestBed, fakeAsync, tick, ComponentFixture } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';
import { Http } from '@angular/http';
import { Router } from '@angular/router';
import { BehaviorSubject } from 'rxjs/BehaviorSubject';

import { AuthService } from '../core/auth.service';
import { NotificationService } from '../core/notification.service';
import { LogoutComponent } from './logout.component';
import { ConfigService } from '../core/config.service';

describe('Forgot Password Component', () => {
  let fixture: ComponentFixture<LogoutComponent>;
  let comp: LogoutComponent;

  let router: any;
  let auth: AuthService;
  let notification: NotificationService;
  let config: ConfigService;

  let username: DebugElement;
  let password: DebugElement;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [FormsModule],
      declarations: [LogoutComponent],
      providers: [
        {provide: Http, useValue: jasmine.createSpy('http')},
        {provide: Router, useValue: jasmine.createSpyObj('router', ['navigate'])},
        AuthService,
        NotificationService,
        ConfigService,
      ],
    });

    fixture = TestBed.createComponent(LogoutComponent);

    comp = fixture.componentInstance;

    auth = fixture.debugElement.injector.get(AuthService);

    router = fixture.debugElement.injector.get(Router);

    notification = fixture.debugElement.injector.get(NotificationService);

    config = fixture.debugElement.injector.get(ConfigService);

    spyOn(notification, 'error');

    spyOn(notification, 'message');
  });

  it('should logout and redirect to home', fakeAsync(() => {
    spyOn(auth, 'logout');

    comp.ngOnInit();

    expect(auth.logout).toHaveBeenCalled();
    expect(router.navigate).toHaveBeenCalledWith([config.basePath]);
  }));
});
