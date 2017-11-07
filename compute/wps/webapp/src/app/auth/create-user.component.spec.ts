import { DebugElement } from '@angular/core';
import { TestBed, async, fakeAsync, tick, ComponentFixture } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { By } from '@angular/platform-browser';
import { Http } from '@angular/http';

import { AuthService } from '../core/auth.service';
import { NotificationService } from '../core/notification.service';

import { CreateUserComponent } from './create-user.component';

describe('Create User Component', () => {
  let router: any;

  let fixture: ComponentFixture<CreateUserComponent>;
  let comp: CreateUserComponent;

  let auth: AuthService;
  let notification: NotificationService;

  let username: DebugElement;
  let openid: DebugElement;
  let email: DebugElement;
  let password: DebugElement;
  let submit: DebugElement;

  beforeEach(async(() => {
    router = jasmine.createSpyObj('router', ['navigate']);

    TestBed.configureTestingModule({
      imports: [FormsModule],
      declarations: [CreateUserComponent],
      providers: [
        {provide: Router, useValue: router},
        {provide: Http, useValue: jasmine.createSpy('http')},
        AuthService,
        NotificationService,
      ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CreateUserComponent);

    comp = fixture.componentInstance;

    auth = fixture.debugElement.injector.get(AuthService);

    notification = fixture.debugElement.injector.get(NotificationService);

    spyOn(notification, 'error');

    username = fixture.debugElement.query(By.css('#username'));

    openid = fixture.debugElement.query(By.css('#openID'));

    email = fixture.debugElement.query(By.css('#email'));

    password = fixture.debugElement.query(By.css('#password'));

    submit = fixture.debugElement.query(By.css('.btn-success'));
  });

  it('should emit error notification', () => {
    spyOn(auth, 'create').and.returnValue(Promise.reject({
      status: 'failed'
    }));

    comp.onSubmit();

    fixture.detectChanges();

    fixture.whenStable().then(() => {
      expect(notification.error).toHaveBeenCalled(); 
    });
  });

  it('should redirect', fakeAsync(() => {
    spyOn(auth, 'create').and.returnValue(Promise.resolve({
      status: 'success'
    }));

    username.nativeElement.value = 'test';
    username.nativeElement.dispatchEvent(new Event('input'));

    openid.nativeElement.value = 'http://test.com/openid/test';
    openid.nativeElement.dispatchEvent(new Event('input'));

    email.nativeElement.value = 'test@gmail.com';
    email.nativeElement.dispatchEvent(new Event('input'));

    password.nativeElement.value = 'test_password';
    password.nativeElement.dispatchEvent(new Event('input'));

    fixture.detectChanges();

    expect(submit.properties.disabled).toBe(false);

    comp.onSubmit();

    tick();

    fixture.detectChanges();
    
    expect(auth.create).toHaveBeenCalled();
    expect(router.navigate).toHaveBeenCalled();
  }));

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
