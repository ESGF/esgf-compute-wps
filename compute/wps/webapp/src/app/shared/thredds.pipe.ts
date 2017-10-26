import { Pipe, PipeTransform } from '@angular/core';

@Pipe({name: 'thredds'})
export class ThreddsPipe implements PipeTransform {
  transform(value: string): string {
    let thredds = value.toLowerCase().indexOf('thredds') >= 0;

    return (thredds ? `${value}.html` : value);
  }
}

