package de.hpi.ddm.data;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.LinkedList;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Line {
    int id;
    String name;
    char[] passwordChars;
    int passwordLength;
    String hashedPassword;
    String cleartextPassword;
    String[] hints;
    LinkedList<Character> charsNotInPasswordFromHints = new LinkedList<>();
}
