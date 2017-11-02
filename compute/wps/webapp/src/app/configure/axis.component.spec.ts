import { DebugElement } from '@angular/core';
import { By } from '@angular/platform-browser';
import { TestBed, ComponentFixture, fakeAsync } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';

import { AxisComponent, Axis } from './axis.component';

describe('Axis Component', () => {
  let fixture: ComponentFixture<AxisComponent>;
  let comp: AxisComponent;

  let id: DebugElement;
  let start: DebugElement;
  let stop: DebugElement;
  let step: DebugElement;
  let title: DebugElement;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [FormsModule],
      declarations: [AxisComponent],
    });

    fixture = TestBed.createComponent(AxisComponent);

    comp = fixture.componentInstance;
  });

  it('should set axis values', fakeAsync(() => {
    comp.axisIndex = 0;

    comp.axis = {
      id: 'latitude',
      id_alt: 'lat',
      units: 'degrees',
      start: 100,
      stop: 200,
      step: 2,
    } as Axis;

    fixture.detectChanges();

    fixture.whenStable().then(() => {
      start = fixture.debugElement.query(By.css('#start0'));

      stop = fixture.debugElement.query(By.css('#stop0'));

      step = fixture.debugElement.query(By.css('#step0'));

      expect(start.nativeElement.value).toBe('100');

      expect(stop.nativeElement.value).toBe('200');

      expect(step.nativeElement.value).toBe('2');
    });
  }));
});
