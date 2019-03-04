import { NotificationService, NotificationType } from './notification.service';

describe('NotificationService', () => {

  beforeEach(() => {
    this.notificationService = new NotificationService();
  });

  it('should emit message', () => {
    let result: any;
    
    this.notificationService.notification$.subscribe((value: any) => result = value);

    this.notificationService.message('message');

    expect(result).toBeDefined();
    expect(result.type).toBe(NotificationType.Message);
    expect(result.text).toBe('message');
  });

  it('should emit warning', () => {
    let result: any;

    this.notificationService.notification$.subscribe((value: any) => result = value);

    this.notificationService.warn('warning');

    expect(result).toBeDefined();
    expect(result.type).toBe(NotificationType.Warn);
    expect(result.text).toBe('warning');
  });

  it('should emit error', () => {
    let result: any;

    this.notificationService.notification$.subscribe((value: any) => result = value);

    this.notificationService.error('error');

    expect(result).toBeDefined();
    expect(result.type).toBe(NotificationType.Error);
    expect(result.text).toBe('error');
  });
});
