package invoice;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.DataSource;

public class DAO {

	private final DataSource myDataSource;

	/**
	 *
	 * @param dataSource la source de données à utiliser
	 */
	public DAO(DataSource dataSource) {
		this.myDataSource = dataSource;
	}

	/**
	 * Renvoie le chiffre d'affaire d'un client (somme du montant de ses factures)
	 *
	 * @param id la clé du client à chercher
	 * @return le chiffre d'affaire de ce client ou 0 si pas trouvé
	 * @throws SQLException
	 */
	public float totalForCustomer(int id) throws SQLException {
		String sql = "SELECT SUM(Total) AS Amount FROM Invoice WHERE CustomerID = ?";
		float result = 0;
		try (Connection connection = myDataSource.getConnection();
			PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setInt(1, id); // On fixe le 1° paramètre de la requête
			try (ResultSet resultSet = statement.executeQuery()) {
				if (resultSet.next()) {
					result = resultSet.getFloat("Amount");
				}
			}
		}
		return result;
	}

	/**
	 * Renvoie le nom d'un client à partir de son ID
	 *
	 * @param id la clé du client à chercher
	 * @return le nom du client (LastName) ou null si pas trouvé
	 * @throws SQLException
	 */
	public String nameOfCustomer(int id) throws SQLException {
		String sql = "SELECT LastName FROM Customer WHERE ID = ?";
		String result = null;
		try (Connection connection = myDataSource.getConnection();
			PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setInt(1, id);
			try (ResultSet resultSet = statement.executeQuery()) {
				if (resultSet.next()) {
					result = resultSet.getString("LastName");
				}
			}
		}
		return result;
	}

	/**
	 * Transaction permettant de créer une facture pour un client
	 *
	 * @param customer Le client
	 * @param productIDs tableau des numéros de produits à créer dans la facture
	 * @param quantities tableau des quantités de produits à facturer faux sinon Les deux tableaux doivent avoir la même
	 * taille
	 * @throws java.lang.Exception si la transaction a échoué
	 */
	public void createInvoice(CustomerEntity customer, int[] productIDs, int[] quantities)
		throws Exception {
		
            int IDCustomer = customer.getCustomerId(); // ID du client
            ResultSet result; //clef générée lors de la création de la facture
            int item =1; // clef primaire du numéro de ligne (première ligne)
            int clef; // clé de la commande
            int cout; // cout total d'une commande de n produits
            
            // commande d'insertion de la facture
            String sqlInvoice = "INSERT INTO Invoice(CustomerID) VALUES(?)";
            
            String sqlItem = "INSERT INTO Item VALUES(?, ?, ?, ?, ?)";
            
            String sqlCout = "SELECT Price FROM Product WHERE ID = ? ";
                        
		try (Connection connection = myDataSource.getConnection();
			PreparedStatement stmt = 
                                connection.prepareStatement(
                                        sqlInvoice, 
                                        Statement.RETURN_GENERATED_KEYS);
                        PreparedStatement stmtItem = 
                                connection.prepareStatement(sqlItem);
                        PreparedStatement stmtCout =
                                connection.prepareStatement(sqlCout)){
                    
                    
                    connection.setAutoCommit(false);
                    
			stmt.setInt(1, IDCustomer);
                        
			int rs = stmt.executeUpdate();
                        
                        if(rs != 1){
                            throw new IllegalArgumentException();
                        }
                        
                        // Les clefs autogénérées sont retournées sous forme de
                        // ResultSet, car il se peut qu'une requête génère
                        // plusieurs clés
                        result = stmt.getGeneratedKeys(); 
                        
                        result.next(); // On lit la première clé générée
                        
                        
                        // Récupération de la clé 
                        clef = result.getInt(1);
                        System.out.println("La première clef autogénérée vaut " 
                                + clef);
                        // Les clés auto-générées sont en général des entiers
                        
                        
                        for(int i =0; i < productIDs.length; i++){
                
                            // Récupération du prix
                            stmtCout.setInt(1, productIDs[i]);
                            result = stmtCout.executeQuery();
                            result.next();
                            cout = result.getInt("Price");
                            
                            
                            stmtItem.setInt(1, clef);
                            stmtItem.setInt(2, item);
                            stmtItem.setInt(3, productIDs[i]);
                            stmtItem.setInt(4, quantities[i]);
                            stmtItem.setInt(5, cout);
                            
                            
                            rs = stmtItem.executeUpdate();
                        
                            if(rs != 1){
                                throw new IllegalArgumentException();
                            }
                            
                            item++;
                        }
                        
                        connection.commit();
                        
		} catch(Exception e){
                    
                }
                
            
            
	}

	/**
	 *
	 * @return le nombre d'enregistrements dans la table CUSTOMER
	 * @throws SQLException
	 */
	public int numberOfCustomers() throws SQLException {
		int result = 0;

		String sql = "SELECT COUNT(*) AS NUMBER FROM Customer";
		try (Connection connection = myDataSource.getConnection();
			Statement stmt = connection.createStatement()) {
			ResultSet rs = stmt.executeQuery(sql);
			if (rs.next()) {
				result = rs.getInt("NUMBER");
			}
		}
		return result;
	}

	/**
	 *
	 * @param customerId la clé du client à recherche
	 * @return le nombre de bons de commande pour ce client (table PURCHASE_ORDER)
	 * @throws SQLException
	 */
	public int numberOfInvoicesForCustomer(int customerId) throws SQLException {
		int result = 0;

		String sql = "SELECT COUNT(*) AS NUMBER FROM Invoice WHERE CustomerID = ?";

		try (Connection connection = myDataSource.getConnection();
			PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setInt(1, customerId);
			ResultSet rs = stmt.executeQuery();
			if (rs.next()) {
				result = rs.getInt("NUMBER");
			}
		}
		return result;
	}

	/**
	 * Trouver un Customer à partir de sa clé
	 *
	 * @param customedID la clé du CUSTOMER à rechercher
	 * @return l'enregistrement correspondant dans la table CUSTOMER, ou null si pas trouvé
	 * @throws SQLException
	 */
	CustomerEntity findCustomer(int customerID) throws SQLException {
		CustomerEntity result = null;

		String sql = "SELECT * FROM Customer WHERE ID = ?";
		try (Connection connection = myDataSource.getConnection();
			PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setInt(1, customerID);

			ResultSet rs = stmt.executeQuery();
			if (rs.next()) {
				String name = rs.getString("FirstName");
				String address = rs.getString("Street");
				result = new CustomerEntity(customerID, name, address);
			}
		}
		return result;
	}

	/**
	 * Liste des clients localisés dans un état des USA
	 *
	 * @param state l'état à rechercher (2 caractères)
	 * @return la liste des clients habitant dans cet état
	 * @throws SQLException
	 */
	List<CustomerEntity> customersInCity(String city) throws SQLException {
		List<CustomerEntity> result = new LinkedList<>();

		String sql = "SELECT * FROM Customer WHERE City = ?";
		try (Connection connection = myDataSource.getConnection();
			PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setString(1, city);
			try (ResultSet rs = stmt.executeQuery()) {
				while (rs.next()) {
					int id = rs.getInt("ID");
					String name = rs.getString("FirstName");
					String address = rs.getString("Street");
					CustomerEntity c = new CustomerEntity(id, name, address);
					result.add(c);
				}
			}
		}

		return result;
	}
}
