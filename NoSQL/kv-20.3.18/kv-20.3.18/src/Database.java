import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import oracle.kv.Direction;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;

import oracle.kv.Key;
import oracle.kv.KeyValueVersion;
import oracle.kv.Value;
import oracle.kv.ValueVersion;

public class Database {

  public static void main(String[] args) throws Exception {
    String[] hhosts = {"127.0.0.1:5000"};
    KVStoreConfig kconfig = new KVStoreConfig("kvstore", hhosts);
    try (KVStore kvstore = KVStoreFactory.getStore(kconfig)) {
      // Cargar los datos del archivo a la base de datos, utilizando el esquema de llaves
      loadData(kvstore);
      
      // Consultar a una persona dada por apellido y nombre.
      System.out.println("\n\nII. Consultar a una persona dada por apellido y nombre");
      searchByLastNameAndFirstName("Soto", "Mariana", kvstore);
      
      // Obtener el teléfono y el correo electrónico de una persona.
      System.out.println("\n\nIII. Obtener el teléfono y el correo electrónico de una persona");
      getTelEmailByID("6738", kvstore);

      // // Obtener el nombre, teléfono y correo electrónico de todas las mujeres
      System.out.println("\n\nIV. Obtener el nombre, teléfono y correo electrónico de todas las mujeres");
      searchByGender("women", kvstore);

      // // Obtener el nombre y el correo electrónico de todas las personas con un mismo apellido.
      System.out.println("\n\nV. Obtener el nombre y el correo electrónico de todas las personas con un mismo apellido");
      getNameEmailByLastName("Barrantes", kvstore); // **

      // // Eliminar todos los registros con el mismo un mismo apellido, en un rango de nombres.
      System.out.println("\n\nVI. Eliminar todos los registros con el mismo un mismo apellido, en un rango de nombres");
      deletePersonByNameRange("Soto", "Liliana", "Ulate", kvstore);
      
      // // Eliminar todos los registros de la base de datos
      System.out.println("\n\nVII. Eliminar todos los registros de la base de datos");
      deleteAll(kvstore);

      // Cerrar kvstore
      kvstore.close();
    }
  }

  public static void loadData(KVStore kvstore) {
    int rows = 2497; // registers on file
    int cols = 7;    // attributes of registers

    String csvPath = "/mnt/e/Universidad/BD_Avanzadas/tarea3/kv-20.3.18/kv-20.3.18/src/people.csv";
    String[][] registers = getCsvMatrix(csvPath, rows, cols);

    for (int i = 1; i < registers.length; i++) {
      ArrayList<String> majorComponents = new ArrayList<>();
      ArrayList<String> minorComponents = new ArrayList<>();

      // save info of person
      String id = registers[i][0];
      String firstName = registers[i][1];
      String lastName = registers[i][2];
      String gender;
      if (registers[i][3].equals("M")) {
        gender = "men";
      } else {
        gender = "women";
      }
      String birthdate = registers[i][4];
      String tel = registers[i][5];
      String email = registers[i][6];

      // Components to genderKey - index to gender = /index/gender/ - /id/
      majorComponents.add("index");
      majorComponents.add(gender);
      minorComponents.add("id");
      minorComponents.add(id);

      Key genderKey = Key.createKey(majorComponents, minorComponents);
      Value idValue = Value.createValue(id.getBytes());

      try {
        kvstore.put(genderKey, idValue);
      } catch (Exception e) {
        e.printStackTrace();
        System.out.println("Failed to insert ID in gender index: " + id);
      }

      // Components to personKey - value
      //    /index/gender/id/ - /id/      /index/gender/id/ - /last_name/
      //    /index/gender/id/ - /name/    /index/gender/id/ - /birthdate/
      //    /index/gender/id/ - /contact_info/
      majorComponents.clear(); minorComponents.clear();
      majorComponents.add("index");
      majorComponents.add(gender);
      majorComponents.add(id);

      String[][] allValues = {
        {"id", id},
        {"name", firstName},
        {"last_name", lastName},
        {"gender", gender.equals("men") ? "M" : "F"},
        {"birthdate", birthdate},
        {"contact_info", "{\"phone_number\": \"" + tel + "\", \"email\": \"" + email + "\"}"}
      };

      for (String[] field : allValues) {
        minorComponents.clear();
        minorComponents.add(field[0]);
        Key recordKey = Key.createKey(majorComponents, minorComponents);
        Value recordValue = Value.createValue(field[1].getBytes());

        try {
          // String str = new String(recordValue.getValue(), StandardCharsets.UTF_8);
          kvstore.put(recordKey, recordValue);
          // System.out.println("Inserted " + field[0] + " in path: " + recordKey.toString() + ": " + str);
        } catch (Exception e) {
          e.printStackTrace();
          System.out.println("Failed to insert " + field[0] + " for ID " + id);
        }
      }

      // Actualizar clave de apellido con el nuevo ID en una lista
      addLastNameKey(lastName, id, kvstore);

      // Actualizar clave de género (hombre/mujer) con el nuevo ID en una lista
      addGenderKey(gender, id, kvstore);
    }
  }

  private static void addLastNameKey(String lastName, String id, KVStore kvstore) {
    ArrayList<String> majorComponents = new ArrayList<>();
    majorComponents.add("index_ln"); 
    majorComponents.add(lastName);
    Key myKey = Key.createKey(majorComponents);

    Value value = null;
    if (kvstore.get(myKey) != null) {
      value = kvstore.get(myKey).getValue();
    }

    String listId = "";
    if (value != null) {
      listId = new String(value.getValue());
    }

    // Agregar el nuevo ID a la lista, evitando duplicados
    if (!listId.contains(id)) {
      if (listId.isEmpty()) {
        listId = id;
      } else {
        listId = listId + "," + id;
      }
      Value updV = Value.createValue(listId.getBytes());
      kvstore.put(myKey, updV);
    }
  }

  private static void addGenderKey(String gender, String id, KVStore kvstore) {
    ArrayList<String> majorComponents = new ArrayList<>();
    majorComponents.add(gender);
    Key myKey = Key.createKey(majorComponents);

    Value value = null;
    if (kvstore.get(myKey) != null) {
      value = kvstore.get(myKey).getValue();
    }

    String listId = "";
    if (value != null) {
      listId = new String(value.getValue());
    }

    // Add id
    if (!listId.contains(id)) {
      if (listId.isEmpty()) {
        listId = id;
      } else {
        listId = listId + "," + id;
      }
      Value updV = Value.createValue(listId.getBytes());
      kvstore.put(myKey, updV);
    }
  }

  public static String[][] getCsvMatrix(String csvFile, int rows, int cols) {
    String archivoCSV = csvFile;
    String linea;
    String separador = ","; 
    String[][] elementos = new String[rows][cols];
    int iterador = 0;
    try (BufferedReader br = new BufferedReader(new FileReader(archivoCSV))) {
      while ((linea = br.readLine()) != null) {
        // Dividir cada línea usando el delimitador
        String[] valores = linea.split(separador);
        
        // Copy array
        System.arraycopy(valores, 0, elementos[iterador], 0, cols);
        iterador++;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    
    return elementos;
  }

  public static void searchByLastNameAndFirstName(String lastName, String firstName, KVStore kvstore) {
    filterPersonByGender("women", lastName, firstName, kvstore);
    filterPersonByGender("men", lastName, firstName, kvstore);
  }

  private static void filterPersonByGender(String gender, String lastName, String firstName, KVStore kvstore) {
    ArrayList<String> majorComponents = new ArrayList<>();
    majorComponents.add("index");
    majorComponents.add(gender);
    Key myKey = Key.createKey(majorComponents);
    
    Iterator<KeyValueVersion> iterator = kvstore.multiGetIterator(Direction.FORWARD
      , 0, myKey, null, null);

    // Iterator to persons
    while (iterator.hasNext()) {
      KeyValueVersion idKvv = iterator.next();
      String id = new String(idKvv.getValue().getValue());

      ArrayList<String> majorComponentsP = new ArrayList<>();
      majorComponentsP.add("index");
      majorComponentsP.add(gender);
      majorComponentsP.add(id);

      Key keyPerson = Key.createKey(majorComponentsP);

      Iterator<KeyValueVersion> iteratorValues = kvstore.multiGetIterator(Direction.FORWARD
        , 0, keyPerson, null, null);

      String firstNameP = null;
      String lastNameP = null;
      String data = "";

      // Get info
      while (iteratorValues.hasNext()) {
        KeyValueVersion personKvv = iteratorValues.next();
        String field = personKvv.getKey().getMinorPath().get(0);
        String value = new String(personKvv.getValue().getValue());
        
        if (field.equals("name")) {
          firstNameP = value;
        } else if (field.equals("last_name")) {
          lastNameP = value;
        }
        data += field + ": " + value + ", ";
      }

      // Comprobar si el nombre y apellido coinciden con los criterios de búsqueda
      if (data != null && firstName.equals(firstNameP) && lastName.equals(lastNameP)) {
        System.out.println(data);
      }
    }
  }

  public static void getTelEmailByID(String id, KVStore kvstore) {
    boolean found = filterByGender("women", id, kvstore);
    if (!found) {
      filterByGender("men", id, kvstore);
    }
  }

  public static boolean filterByGender(String gender, String id, KVStore kvstore) {
    boolean found = false;

    ArrayList<String> majorComponentsP = new ArrayList<>();
    ArrayList<String> minorComponentsP = new ArrayList<String>();
    majorComponentsP.add("index");
    majorComponentsP.add(gender);
    majorComponentsP.add(id);
    minorComponentsP.add("contact_info");
    Key myKey = Key.createKey(majorComponentsP, minorComponentsP);

    ValueVersion vv = kvstore.get(myKey);
    String value = new String(vv.getValue().getValue());
    if (vv != null && value != null) {
      String tel = extractEmail(value);
      String email = extractTel(value);
      System.out.println("Tel: " + tel + ", Email: " + email);
      found = true;
    }

    return found;
  }

  public static void searchByGender(String gender, KVStore kvstore) {
    ArrayList<String> majorComponents = new ArrayList<>();
    majorComponents.add("index");
    majorComponents.add(gender);
    Key myKey = Key.createKey(majorComponents);

    Iterator<KeyValueVersion> iterator = kvstore.multiGetIterator(Direction.FORWARD
      , 0, myKey, null, null);

    // Iterator for each id
    while (iterator.hasNext()) {
      KeyValueVersion idKvv = iterator.next();
      String id = new String(idKvv.getValue().getValue());

      ArrayList<String> majorComponentsP = new ArrayList<>();
      majorComponentsP.add("index");
      majorComponentsP.add(gender);
      majorComponentsP.add(id);

      Key keyPerson = Key.createKey(majorComponentsP);

      // Get person data
      Iterator<KeyValueVersion> iteratorValues = kvstore.multiGetIterator(Direction.FORWARD
        , 0, keyPerson, null, null);

      String name = null;
      String tel = null;
      String email = null;

      // Get name - tel - email
      while (iteratorValues.hasNext()) {
        KeyValueVersion personKvv = iteratorValues.next();
        String field = personKvv.getKey().getMinorPath().get(0);
        String value = new String(personKvv.getValue().getValue());

        if (field.equals("name")) {
          name = value;
        } else if (field.equals("contact_info")) {
          tel = extractTel(value);
          email = extractEmail(value);
        }
      }

      if (name != null && email != null && tel != null) {
        System.out.println("Name: " + name + ", Tel: " + tel + ", Email: " + email);
      }
    }
  }

  public static String extractTel(String data) {
    String[] parts = data.split("\"phone_number\": \"");
    if (parts.length > 1) {
      return parts[1].split("\"")[0];
    }
    return null;
  }

  public static void getNameEmailByLastName(String lastName, KVStore kvstore) {
    // Consultar y mostrar contactos para mujeres con el lastName dado
    filterGetEmailSameLNByGender("women", lastName, kvstore);

    // Consultar y mostrar contactos para hombres con el lastName dado
    filterGetEmailSameLNByGender("men", lastName, kvstore);
  }

  public static void filterGetEmailSameLNByGender(String gender, String lastName, KVStore kvstore) {
    // Definir el major path para el género y lastName especificados
    ArrayList<String> majorComponents = new ArrayList<>();
    majorComponents.add("index");
    majorComponents.add(gender);
    Key myKey = Key.createKey(majorComponents);

    Iterator<KeyValueVersion> iterator = kvstore.multiGetIterator(Direction.FORWARD
      , 0, myKey, null, null);
    
    // Iterator for each id
    while (iterator.hasNext()) {
      KeyValueVersion idKvv = iterator.next();
      String id = new String(idKvv.getValue().getValue());

      ArrayList<String> majorComponentsP = new ArrayList<>();
      majorComponentsP.add("index");
      majorComponentsP.add(gender);
      majorComponentsP.add(id);

      Key keyPerson = Key.createKey(majorComponentsP);

      // Get person data
      Iterator<KeyValueVersion> iteratorValues = kvstore.multiGetIterator(Direction.FORWARD
        , 0, keyPerson, null, null);

      String name = null;
      String lastNameV = null;
      String email = null;

      // Get name - email
      while (iteratorValues.hasNext()) {
        KeyValueVersion personKvv = iteratorValues.next();
        String field = personKvv.getKey().getMinorPath().get(0);
        String value = new String(personKvv.getValue().getValue());

        if (field.equals("name")) {
          name = value;
        } else if (field.equals("contact_info")) {
          email = extractEmail(value);
        } else if (field.equals("last_name")) {
          lastNameV = value;
        }
      }

      // Imprimir la información de la persona si se obtuvo el nombre y el correo electrónico
      if (name != null && email != null && lastNameV.equals(lastName)) {
        System.out.println("Name: " + name + " " + lastNameV + ", Email: " + email);
      }
    }
  }

  public static String extractEmail(String data) {
    String[] parts = data.split("\"email\": \"");
    if (parts.length > 1) {
      return parts[1].split("\"")[0];
    }
    return null;
  }

  public static void deletePersonByNameRange(String lastName, String firstRangeName
        , String lastRangeName, KVStore kvstore) {
    
    System.out.println("Deleting registers with: " + lastName + " in range from: "
      + firstRangeName + " to " + lastRangeName);
    // Key: lastName, Value: ID's
    ArrayList<String> majorComponents = new ArrayList<>();
    majorComponents.add("index_ln");  
    majorComponents.add(lastName);
    Key lastNameKey = Key.createKey(majorComponents);

    // Get values
    Value idListValue;
    if (kvstore.get(lastNameKey) != null) {
      idListValue = kvstore.get(lastNameKey).getValue();
    } else {
      System.out.println("No contact with: " + lastName);
      return;
    }

    // Convertir la lista de IDs a un array para iterar sobre cada persona
    String[] listId = new String(idListValue.getValue()).split(",");

    for (String id : listId) {
      ArrayList<String> majorComponentsMen = new ArrayList<>();
      majorComponentsMen.add("index");
      majorComponentsMen.add("men");
      majorComponentsMen.add(id);
      Key keyMen = Key.createKey(majorComponentsMen);

      ArrayList<String> majorComponentsWomen = new ArrayList<>();
      majorComponentsWomen.add("index");
      majorComponentsWomen.add("Women");
      majorComponentsWomen.add(id);
      Key keyWomen = Key.createKey(majorComponentsWomen);

      // Verificar si el nombre de la persona está en el rango y eliminar si corresponde
      if (nameInRange(keyMen, lastName, firstRangeName, lastRangeName, kvstore)) {
        deletePerson(keyMen, kvstore);
      }
      if (nameInRange(keyWomen, lastName, firstRangeName, lastRangeName, kvstore)) {
        deletePerson(keyWomen, kvstore);
      }
    }
  }

    // Función para verificar si el nombre de una persona está en el rango de nombres especificado
  private static boolean nameInRange(Key personKey, String lastName, String firstRange
        , String lastRange, KVStore kvstore) {
    
    Iterator<KeyValueVersion> iterator = kvstore.multiGetIterator(Direction.FORWARD
      , 0, personKey, null, null);
    String personName = null;
    String personLastName = null;

    while (iterator.hasNext()) {
      KeyValueVersion kvv = iterator.next();
      String field = kvv.getKey().getMinorPath().get(0);
      String value = new String(kvv.getValue().getValue());

      if (field.equals("name")) {
        personName = value;
      } else if (field.equals("last_name")) {
        personLastName = value;
      }
    }

    // Verificar si el lastName coincide y el nombre está en el rango
    boolean isInRange = false;
    if ((personLastName != null) && (personLastName.equals(lastName))
        && (personName != null) && (personName.compareTo(firstRange) >= 0)
        && (personName.compareTo(lastRange) <= 0)) {
      isInRange = true;
    }
    
    return isInRange;
  }

    // Función para eliminar todos los campos asociados a una persona específica dado su Key
  private static void deletePerson(Key personKey, KVStore kvstore) {
    Iterator<KeyValueVersion> iterator = kvstore.multiGetIterator(Direction.FORWARD
      , 0, personKey, null, null);

    while (iterator.hasNext()) {
      KeyValueVersion kvv = iterator.next();
      kvstore.delete(kvv.getKey());
    }
  }

    // Full clean
  public static void deleteAll(KVStore kvstore) {
    System.out.println("Deleting all registers on database");

    // Delete from index
    ArrayList<String> indexComponents = new ArrayList<>();
    indexComponents.add("index");
    Key indexKey = Key.createKey(indexComponents);
    kvstore.multiDelete(indexKey, null, null);
    // Delete from index men
    ArrayList<String> menComponents = new ArrayList<>();
    menComponents.add("men");
    Key menKey = Key.createKey(menComponents);
    kvstore.multiDelete(menKey, null, null);
    // Delete from index women
    ArrayList<String> womenComponents = new ArrayList<>();
    womenComponents.add("women");
    Key womenKey = Key.createKey(womenComponents);
    kvstore.multiDelete(womenKey, null, null);
    // Delete from index lastName
    ArrayList<String> lastNameIndexComponents = new ArrayList<>();
    lastNameIndexComponents.add("index_ln");  
    Key lastNameIndexKey = Key.createKey(lastNameIndexComponents);
    kvstore.multiDelete(lastNameIndexKey, null, null);

    System.out.println("Finished successfully");
  }
}
