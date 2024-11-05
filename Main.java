import java.util.*;
class Main {

    // Method to find the First Non-Repeating Character in a String
    public static Character findNonRepeating(String s){

        // Creating the HashMap to store the frequencies of each character
        HashMap<Character, Integer> map = new HashMap<>();

        // Traversing each character in a string and store its frequencies
        for(int i=0; i<s.length(); i++){
            map.put(s.charAt(i), map.getOrDefault(s.charAt(i), 0) + 1);
        }

        // Again Traversing each character in the string to find the first non-repeating character
        for(char c : s.toCharArray()){
            if(map.get(c) == 1){
            return c;
            }
        }

        // Returning null if no non-repeating characters found
        return null;
    }

    public static void main(String[] args) {

        // Testing the findNonRepeating method with different strings
        char c1 = findNonRepeating("abcdef");
        System.out.println(c1);
        char c2 = findNonRepeating("apple");
        System.out.println(c2);
        char c3 = findNonRepeating("aabbccdd");
        System.out.println(c3);
    }
}