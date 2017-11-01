import { TestBed, async } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { Http } from '@angular/http';

import { SharedModule } from '../shared/shared.module';
import { StatsService, FileStat } from '../shared/stats.service';

import { AdminFilesComponent } from './admin-files.component';

describe('Admin Files Component', () => {
  beforeEach(async(() => {
    let data: FileStat[] = [
      {
        name: 'test',
        host: 'test',
        url: 'url',
        variable: 'tas',
        requested: 2,
        requested_date: Date.now()
      }
    ];

    TestBed.configureTestingModule({
      imports: [SharedModule],
      declarations: [AdminFilesComponent],
      providers: [
        {provide: Http, useValue: jasmine.createSpy('http')},
        {provide: StatsService, useClass: class {
          files(): Promise<FileStat[]> {
            return new Promise<FileStat[]>((resolve, reject) => {
              resolve(data);
            });
          }
        }},
      ],
    })
    .compileComponents();
  }));

  beforeEach(() => {
    this.fixture = TestBed.createComponent(AdminFilesComponent);

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
