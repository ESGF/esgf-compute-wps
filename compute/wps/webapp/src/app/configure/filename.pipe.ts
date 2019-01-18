import { Pipe, PipeTransform } from '@angular/core';

@Pipe({name: 'filename'})
export class FilenamePipe implements PipeTransform {
  transform(value: any) {
    let parts;

    try {
      parts = value.split('/');
    } catch (TypeError) {
      return value;
    }

    return parts[parts.length-1];
  }
}
