import { Pipe, PipeTransform } from '@angular/core';

import { Variable } from './variable';

@Pipe({name: 'filter'})
export class FilterPipe implements PipeTransform {
  transform(value: Variable[], filter: string) {
    if (value == undefined) {
      return;
    }

    let pattern = new RegExp(filter);

    return value.filter((item: Variable) => {
      return item.file.match(pattern) != null; 
    });
  }
}
