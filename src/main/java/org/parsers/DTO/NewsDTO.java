package org.parsers.DTO;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public record NewsDTO(String link, String hash) {
    private static String getHash(String input) throws NoSuchAlgorithmException {
        var digest = MessageDigest.getInstance("SHA-256");
        byte[] hashBytes = digest.digest(input.getBytes(StandardCharsets.UTF_8));
        return Base64.getEncoder().encodeToString(hashBytes);
    }

    public static NewsDTO fromLink(String link) throws NoSuchAlgorithmException {
        return new NewsDTO(link, getHash(link));
    }
}
