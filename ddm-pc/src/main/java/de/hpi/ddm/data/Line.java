package de.hpi.ddm.data;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.LinkedList;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Line {
    int id;
    String name;
    List<String> passwordChars;
    int passwordLength;
    String hashedPassword;
    String cleartextPassword;
    List<String> hints;
    LinkedList<Character> charsNotInPasswordFromHints = new LinkedList<>();
}
