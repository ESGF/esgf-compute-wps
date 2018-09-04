import { Pipe, PipeTransform } from '@angular/core';

@Pipe({name: 'filter'})
export class FilterPipe implements PipeTransform {
  transform(value: any[], filter: string) {
    let pattern = new RegExp(filter);

    return value.filter((item: any) => {
      return item.url.match(pattern) != null; 
    });
  }
}
