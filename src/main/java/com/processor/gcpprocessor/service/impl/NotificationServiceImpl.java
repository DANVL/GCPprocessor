package com.processor.gcpprocessor.service.impl;

import com.processor.gcpprocessor.service.NotificationService;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import java.util.ArrayDeque;
import java.util.Deque;

@Service
@Scope("singleton")
public class NotificationServiceImpl implements NotificationService {

    private final Deque<String> notifications;

    public NotificationServiceImpl() {
        this.notifications = new ArrayDeque<>();
    }

    @Override
    public void addNotification(String notification) {
        notifications.addLast(notification);
    }

    @Override
    public String getNotifications() {
        StringBuilder result = new StringBuilder();
        for (String notification : notifications) {
            result.append(notification).append(System.lineSeparator());
        }

        return result.toString().equals("") ? "No notifications ¯\\_(ツ)_/¯" : result.toString();
    }
}
