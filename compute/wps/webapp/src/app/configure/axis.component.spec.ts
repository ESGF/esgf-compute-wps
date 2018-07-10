import { DebugElement } from '@angular/core';
import { By } from '@angular/platform-browser';
import { TestBed, ComponentFixture } from '@angular/core/testing';
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
});
