import { TestBed, async } from '@angular/core/testing';
import { Directive, NO_ERRORS_SCHEMA }          from '@angular/core';
import { Http } from '@angular/http';
import { By } from '@angular/platform-browser';
import { RouterModule, Router } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { AppComponent } from './app.component';

import { AuthService } from './core/auth.service';
import { NotificationService } from './core/notification.service';
import { WPSService } from './core/wps.service';

import { User } from './user/user.service';

@Directive({
      selector: '[routerLink], [routerLinkActive]'
})
class DummyRouterLinkDirective {}

describe('App Component', () => {
  beforeEach(async(() => {
    this.router = jasmine.createSpyObj('router', ['navigate']);
    this.wps = jasmine.createSpy('wps');
    this.http = jasmine.createSpy('http');

    TestBed.configureTestingModule({
      declarations: [AppComponent, DummyRouterLinkDirective],
      providers: [
        {provide: WPSService, useValue: this.wps},
        {provide: Http, useValue: this.http},
        {provide: Router, useValue: this.router},
        AuthService,
        NotificationService,
      ],
      schemas: [NO_ERRORS_SCHEMA],
    })
    .compileComponents();

    this.auth = TestBed.get(AuthService);
    this.notification = TestBed.get(NotificationService);
  }));

  beforeEach(() => {
    this.fixture = TestBed.createComponent(AppComponent);

    this.comp = this.fixture.componentInstance;
  });

  it('should display a error', () => {
    let text = 'text';

    this.notification.error(text);

    this.fixture.detectChanges()

    let de = this.fixture.debugElement.query(By.css('.alert-danger'));

    expect(de.classes.hidden).toBe(false);
  });

  it('should display a warning', () => {
    let text = 'text';

    this.notification.warn(text);

    this.fixture.detectChanges()

    let de = this.fixture.debugElement.query(By.css('.alert-warning'));

    expect(de.classes.hidden).toBe(false);
  });

  it('should display a message', () => {
    let text = 'text';

    this.notification.message(text);

    this.fixture.detectChanges()

    let de = this.fixture.debugElement.query(By.css('.alert-success'));

    expect(de.classes.hidden).toBe(false);
  });

  it('should display no messages, warnings or errors', () => {
    this.fixture.detectChanges();

    let notification = this.fixture.debugElement.query(By.css('.alert-info'))

    expect(notification.classes.hidden).toBe(true);

    let message = this.fixture.debugElement.query(By.css('.alert-success'));

    expect(message.classes.hidden).toBe(true);

    let warning = this.fixture.debugElement.query(By.css('.alert-warning'));

    expect(message.classes.hidden).toBe(true);

    let error = this.fixture.debugElement.query(By.css('.alert-danger'));

    expect(error.classes.hidden).toBe(true);
  });

  it('should show profile, jobs and logout links', () => {
    this.auth.setLoggedIn(true);

    this.fixture.detectChanges();

    let de = this.fixture.debugElement.query(By.css('.profile'));

    expect(de).not.toBe(null);

    de = this.fixture.debugElement.query(By.css('.logout'));

    expect(de).not.toBe(null);

    de = this.fixture.debugElement.query(By.css('.jobs'));

    expect(de).not.toBe(null);
  });

  it('should show login link', () => {
    this.auth.setLoggedIn(false);

    this.fixture.detectChanges();

    let de = this.fixture.debugElement.query(By.css('.login'));

    expect(de).not.toBe(null);
  });
});
