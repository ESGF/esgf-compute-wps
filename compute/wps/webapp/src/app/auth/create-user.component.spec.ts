import { DebugElement } from '@angular/core';
import { TestBed, ComponentFixture, async } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { By } from '@angular/platform-browser';

import { AuthService } from '../core/auth.service';
import { NotificationService } from '../core/notification.service';

import { CreateUserComponent } from './create-user.component';

describe('Create User Component', () => {
  let router: any;
  let notification: any;
  let auth: any;

  let fixture: ComponentFixture<CreateUserComponent>;
  let comp: CreateUserComponent;

  let username: DebugElement;
  let openid: DebugElement;
  let email: DebugElement;
  let password: DebugElement;
  let submit: DebugElement;

  beforeEach(async(() => {
    router = jasmine.createSpyObj('router', ['navigate']);
    notification = jasmine.createSpy('notification');
    auth = jasmine.createSpyObj('auth', ['create']);

    TestBed.configureTestingModule({
      imports: [FormsModule],
      declarations: [CreateUserComponent],
      providers: [
        {provide: Router, useValue: router},
        {provide: NotificationService, useValue: notification},
        {provide: AuthService, useValue: auth},
      ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CreateUserComponent);

    comp = fixture.componentInstance;

    username = fixture.debugElement.query(By.css('#username'));

    openid = fixture.debugElement.query(By.css('#openID'));

    email = fixture.debugElement.query(By.css('#email'));

    password = fixture.debugElement.query(By.css('#password'));

    submit = fixture.debugElement.query(By.css('.btn-success'));
  });

  it('should be valid', () => {
    fixture.detectChanges();
    
    fixture.whenStable().then(() => {
      username.nativeElement.value = 'test';
      username.nativeElement.dispatchEvent(new Event('input'));

      openid.nativeElement.value = 'http://test.com/openid/test';
      openid.nativeElement.dispatchEvent(new Event('input'));

      email.nativeElement.value = 'test@gmail.com';
      email.nativeElement.dispatchEvent(new Event('input'));

      password.nativeElement.value = 'test_password';
      password.nativeElement.dispatchEvent(new Event('input'));

      expect(submit.properties.disabled).toBe(false);

      submit.nativeElement.click();

      expect(auth.create).toHaveBeenCalled();
    });
  });

  it('should set username value', () => {
    fixture.detectChanges();
    
    fixture.whenStable().then(() => {
      username.nativeElement.value = 'test';
      username.nativeElement.dispatchEvent(new Event('input'));

      expect(comp.model.username).toBe('test');
    });
  });

  it('should initialize with blank values', () => {
    fixture.detectChanges();

    expect(username.nativeElement.value).toBe('');
  });
});
