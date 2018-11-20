import { DebugElement } from '@angular/core';
import { By } from '@angular/platform-browser';
import { TestBed, async, fakeAsync, tick, ComponentFixture } from '@angular/core/testing';
import { Http } from '@angular/http';
import { FormsModule } from '@angular/forms';
import { Router, ActivatedRoute } from '@angular/router';

import { Observable } from 'rxjs/Observable';

import { AxisComponent } from './axis.component';
import { AuthService } from '../core/auth.service';
import { NotificationService } from '../core/notification.service';

import { ConfigureService } from './configure.service';
import { ConfigureComponent } from './configure.component';

describe('Configure Component', () => {
  let fixture: ComponentFixture<ConfigureComponent>;
  let comp: ConfigureComponent;

  let route: any;
  let router: any;

  let config: ConfigureService;

  let mockResult = {
    tas: {
      files: ['file1', 'file2'],
      axes: [
        {
          id: 'time',
          id_alt: 't',
          start: 0,
          stop: 3650,
          step: 2,
          units: 'days since 1990-1-1',
        }
      ]
    }
  };

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [FormsModule],
      declarations: [
        AxisComponent,
        ConfigureComponent
      ],
      providers: [
        {provide: Http, useValue: jasmine.createSpy('http')},
        {provide: Router, useValue: jasmine.createSpy('router')},
        AuthService,
        NotificationService,
      ],
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ConfigureComponent);

    comp = fixture.componentInstance;

    config = fixture.debugElement.injector.get(ConfigureService);

    route = fixture.debugElement.injector.get(ActivatedRoute);

    router = fixture.debugElement.injector.get(Router);
  });

  it('should retrieve axes for selected variable', () => {
    //comp.config.variable = 'tas';
  });

  it('should initialize', () => {
    spyOn(config, 'searchESGF').and.returnValue(Promise.resolve(mockResult));

    comp.ngOnInit();

    fixture.detectChanges();

    fixture.whenStable().then(() => {
      fixture.detectChanges();

      let processes = fixture.debugElement.query(By.css('#process'));
      let variables = fixture.debugElement.query(By.css('#variable'));

      expect(processes.children.length).toBe(2);
      expect(variables.children.length).toBe(1);
      //expect(comp.config.dataset.axes.length).toBe(1);
    });
  });
});
