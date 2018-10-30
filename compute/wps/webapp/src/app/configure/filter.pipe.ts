import { Pipe, PipeTransform } from '@angular/core';

@Pipe({name: 'filter'})
export class FilterPipe implements PipeTransform {
  transform(value: any, filter: string) {
    if (value == undefined) {
      return;
    }

    let pattern = new RegExp(filter);

    return value.filter((item: string) => {
      return item.match(pattern) != null; 
    });
  }
}
