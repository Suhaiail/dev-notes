class EventEmitter {
  constructor() {
    this.events = {};
  }

  on(event, listener) {
    (this.events[event] ||= []).push(listener);
    return this;
  }

  emit(event, ...args) {
    (this.events[event] || []).forEach(fn => fn(...args));
    return this;
  }

  off(event, listener) {
    this.events[event] = (this.events[event] || []).filter(fn => fn !== listener);
    return this;
  }
}
