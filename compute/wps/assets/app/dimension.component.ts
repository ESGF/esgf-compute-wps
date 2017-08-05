import { Component, Input, Output, EventEmitter } from '@angular/core';

export class Dimension {
  uuid: number;
  nameEdit: boolean = false;

  @Output() remove: EventEmitter<Dimension> = new EventEmitter();

  constructor(
    public name?: string,
    public unit?: string,
    public start?: number,
    public stop?: number,
    public step?: number
  ) { 
    this.uuid = new Date().getTime();

    this.nameEdit = this.name !== undefined;

    if (this.unit !== undefined) {
      this.name = `${this.name} (${this.unit})`;
    }

    if (this.step === undefined) {
      this.step = 1;
    }
  }
  
  valid(): boolean {
    let values = [this.name, this.start, this.stop, this.step];  

    return values.every((element, index, array) => {
      return element !== undefined && element !== '';
    });
  }

  toString(): string {
    return JSON.stringify(this);
  }
}

@Component({
  selector: 'dimension',
  templateUrl: './dimension.component.html',
  styleUrls: ['./forms.css']
})
export class DimensionComponent {
  @Input() dimension: Dimension;

  onRemoveDimension(): void {
    this.dimension.remove.emit(this.dimension);
  }
}
