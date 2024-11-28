package tp3;
public class Transaction {
   private String userId;
   private double transactionAmount;
   private long timestamp;

   // Getters and setters
   public String getUserId() { return userId; }
   public void setUserId(String userId) { this.userId = userId; }
   public double getTransactionAmount() { return transactionAmount; }
   public void setTransactionAmount(double transactionAmount) { this.transactionAmount = transactionAmount; }
   public long getTimestamp() { return timestamp; }
   public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
}
