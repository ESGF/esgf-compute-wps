import { TestBed, async } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { Http } from '@angular/http';

import { SharedModule } from '../shared/shared.module';
import { StatsService, ProcessStat } from '../shared/stats.service';

import { AdminProcessesComponent } from './admin-processes.component';

describe('Admin Files Component', () => {
  beforeEach(async(() => {
    let data: ProcessStat[] = [
      {
        identifier: 'CDAT.avg',
        backend: 'Local',
        requested: 20,
        requested_date: Date.now()
      }
    ];

    TestBed.configureTestingModule({
      imports: [SharedModule],
      declarations: [AdminProcessesComponent],
      providers: [
        {provide: Http, useValue: jasmine.createSpy('http')},
        {provide: StatsService, useClass: class {
          processes(): Promise<ProcessStat[]> {
            return new Promise<ProcessStat[]>((resolve, reject) => {
              resolve(data);
            });
          }
        }},
      ],
    })
    .compileComponents();
  }));

  beforeEach(() => {
    this.fixture = TestBed.createComponent(AdminProcessesComponent);

    this.comp = this.fixture.componentInstance;
  });

  it('should display files', () => {
    this.fixture.detectChanges();

    let rows = this.fixture.debugElement.queryAll(By.css('tr'));

    expect(rows).toBeDefined();
    expect(rows.length).toBe(1);
  });

  it('should display not files', () => {
    let rows = this.fixture.debugElement.query(By.css('tr'));

    expect(rows).toBeNull();
  });
});
