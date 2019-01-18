import { Pipe, PipeTransform } from '@angular/core';

@Pipe({name: 'enum'})
export class EnumPipe implements PipeTransform {
  transform(value: any): any {
    let enumValues = [];

    for (let x in value) {
      enumValues.push({key: x, value: value[x]});
    }

    return enumValues;;
  }
}
