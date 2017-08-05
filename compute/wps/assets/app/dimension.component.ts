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
  }

  toString(): string {
    return JSON.stringify(this);
  }
}

@Component({
  selector: 'dimension',
  templateUrl: './dimension.component.html'
})
export class DimensionComponent {
  @Input() dimension: Dimension;

  onRemoveDimension(): void {
    this.dimension.remove.emit(this.dimension);
  }
}
