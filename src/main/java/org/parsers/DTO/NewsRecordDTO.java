package org.parsers.DTO;

public record NewsRecordDTO(
        String link,
        String category,
        String title,
        String content,
        String hash
) {
}
