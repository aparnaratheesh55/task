package com.example.task.Service;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Service
public class UserService {

    private static final String USER_FILE_PATH = "/home/aparnaratheesh/Documents/user_details";
    private static final String DEPARTMENT_FILE_PATH = "/home/aparnaratheesh/Documents/user_department";

    public List<Map<String, Object>> getUsersByCity(String city) {
        ObjectMapper objectMapper=new ObjectMapper();
        CompletableFuture<List<Map<String, Object>>> usersFuture = CompletableFuture.supplyAsync(() -> {
            try {
                File file = new File(USER_FILE_PATH);
                return objectMapper.readValue(file, new TypeReference<List<Map<String, Object>>>() {});
            } catch (IOException e) {
                throw new RuntimeException("Failed to load users from file", e);
            }
        });

        CompletableFuture<List<Map<String, Object>>> departmentsFuture = CompletableFuture.supplyAsync(() -> {
            try {
                File file = new File(DEPARTMENT_FILE_PATH);
                return objectMapper.readValue(file, new TypeReference<List<Map<String, Object>>>() {});
            } catch (IOException e) {
                throw new RuntimeException("Failed to load departments from file", e);
            }
        });

        return usersFuture
                .thenCombine(departmentsFuture, (users, departments) -> {
                    return mergeAndFilter(users, departments, city);
                })
                .join();
    }

    private List<Map<String, Object>> mergeAndFilter(List<Map<String, Object>> users, List<Map<String, Object>> departments, String city) {
        return users.stream()
                .filter(user -> user.get("city").toString().equalsIgnoreCase(city))
                .map(user -> {
                    Map<String, Object> department = departments.stream()
                            .filter(dep -> dep.get("userId").equals(user.get("userId")))
                            .findFirst()
                            .orElseThrow(() -> new RuntimeException("Department not found for user: " + user.get("userId")));

                    return Map.of(
                            "name", user.get("name"),
                            "location", user.get("city"),
                            "department", department.get("department")
                    );
                })
                .collect(Collectors.toList());
    }
}
