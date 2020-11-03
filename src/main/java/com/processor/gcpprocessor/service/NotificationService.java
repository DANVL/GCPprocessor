package com.processor.gcpprocessor.service;

import java.util.Deque;

public interface NotificationService {
    void addNotification(String notification);
    String getNotifications();
}
