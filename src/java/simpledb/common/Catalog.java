package simpledb.common;

import simpledb.common.Type;
import simpledb.storage.DbFile;
import simpledb.storage.HeapFile;
import simpledb.storage.TupleDesc;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 *
 * @Threadsafe
 */
public class Catalog {

    public static class Table implements Serializable {

        private static final long serialVersionUID = 1L;

        public DbFile file;

        public String name;

        public String pkey;

        public Table(DbFile file, String name, String pkey) {
            this.file = file;
            this.name = name;
            this.pkey = pkey;
        }
    }

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Maps from table id to table.
     */
    private ConcurrentHashMap<Integer, Table> tables;

    /**
     * Maps from table name to table id.
     */
    private ConcurrentHashMap<String, Integer> tableNames;

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        tableNames = new ConcurrentHashMap<>();
        tables = new ConcurrentHashMap<>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     *
     * @param file      the contents of the table to add;  file.getId() is the identifier of
     *                  this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name      the name of the table -- may be an empty string.  May not be null.  If a name
     *                  conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        lock.writeLock().lock();

        if (tableNames.containsKey(name)) {
            tables.remove(tableNames.get(name));
        }

        tableNames.put(name, file.getId());
        tables.put(file.getId(), new Table(file, name, pkeyField));

        lock.writeLock().unlock();
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     *
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *             this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     *
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        lock.readLock().lock();

        if (name == null || !tableNames.containsKey(name)) {
            lock.readLock().unlock();
            throw new NoSuchElementException();
        }

        int res = tables.get(tableNames.get(name)).file.getId();
        lock.readLock().unlock();
        return res;
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     *
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        lock.readLock().lock();

        if (!tables.containsKey(tableid)) {
            lock.readLock().unlock();
            throw new NoSuchElementException();
        }

        TupleDesc res = tables.get(tableid).file.getTupleDesc();
        lock.readLock().unlock();
        return res;
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     *
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        lock.readLock().lock();

        if (!tables.containsKey(tableid)) {
            lock.readLock().unlock();
            throw new NoSuchElementException();
        }

        DbFile res = tables.get(tableid).file;
        lock.readLock().unlock();
        return res;
    }

    public String getPrimaryKey(int tableid) {
        lock.readLock().lock();
        String res = tables.get(tableid).pkey;
        lock.readLock().unlock();
        return res;
    }

    public Iterator<Integer> tableIdIterator() {
        lock.readLock().lock();
        Iterator<Integer> res = new ArrayList<>(tableNames.values()).iterator();
        lock.readLock().unlock();
        return res;
    }

    public String getTableName(int id) {
        lock.readLock().lock();
        String res = tables.get(id).name;
        lock.readLock().unlock();
        return res;
    }

    /**
     * Delete all tables from the catalog
     */
    public void clear() {
        lock.writeLock().lock();
        tableNames = new ConcurrentHashMap<>();
        tables = new ConcurrentHashMap<>();
        lock.writeLock().unlock();
    }

    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     *
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder = new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(catalogFile));

            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<>();
                ArrayList<Type> types = new ArrayList<>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().equalsIgnoreCase("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().equalsIgnoreCase("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder + "/" + name + ".dat"), t);
                addTable(tabHf, name, primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

