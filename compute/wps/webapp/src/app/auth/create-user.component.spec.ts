import { TestBed, async } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { By } from '@angular/platform-browser';

import { AuthService } from '../core/auth.service';
import { NotificationService } from '../core/notification.service';

import { CreateUserComponent } from './create-user.component';

describe('Create User Component', () => {
  beforeEach(async(() => {
    this.router = jasmine.createSpyObj('router', ['navigate']);
    this.notification = jasmine.createSpy('notification');
    this.auth = jasmine.createSpy('auth');

    TestBed.configureTestingModule({
      imports: [FormsModule],
      declarations: [CreateUserComponent],
      providers: [
        {provide: Router, useValue: this.router},
        {provide: NotificationService, useValue: this.notification},
        {provide: AuthService, useValue: this.auth},
      ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    this.fixture = TestBed.createComponent(CreateUserComponent);

    this.comp = this.fixture.componentInstance;

    this.username = this.fixture.debugElement.query(By.css('#username'));

    this.openid = this.fixture.debugElement.query(By.css('#openID'));

    this.email = this.fixture.debugElement.query(By.css('#email'));

    this.password = this.fixture.debugElement.query(By.css('#password'));

    this.submit = this.fixture.debugElement.query(By.css('.btn-success'));
  });

  it('should submit', () => {
    this.submit.nativeElement.dispatchEvent(new Event('click'));
  });

  it('should enable submit', () => {
    this.username.value = 'test';
    this.username.triggerEventHandler('input', null);

    this.openid.value = 'http://test.com/openid/test';
    this.openid.nativeElement.dispatchEvent(new Event('input'));

    this.email.value = 'test@gmail.com';
    this.email.nativeElement.dispatchEvent(new Event('input'));

    this.password.value = 'testPassword';
    this.password.nativeElement.dispatchEvent(new Event('input'));

    this.fixture.detectChanges();

    this.fixture.whenStable().then(() => {
      expect(this.submit.properties.disabled).toBe(false);
    });
  });
});
