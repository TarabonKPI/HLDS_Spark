import java.time.LocalDateTime;

public class Order {

  public String id;
  public String driverName;
  public String clientName;
  public Coordinates pickupCoordinates;
  public Coordinates dropOffCoordinates;
  public LocalDateTime pickupDateTime;
  public LocalDateTime dropoffDateTime;
  public float cost;
  public int driverRating;
  public String driverComment;
  public int clientRating;
  public String clientComment;

  // autoincrement
  public String getId() { return id; }
  public void setId(String id) { this.id = id; }

  // driver_reviews [userName]
  public String getDriverName() { return driverName; }
  public void setDriverName(String driverName) { this.driverName = driverName; }

  // customer_reviews [reviews.username]
  public String getClientName() { return clientName; }
  public void setClientName(String clientName) { this.clientName = clientName; }

  // london_postcodes [Postcode, Latitude, Longitude]
  public Coordinates getPickupCoordinates() { return pickupCoordinates; }
  public void setPickupCoordinates(Coordinates pickupCoordinates) { this.pickupCoordinates = pickupCoordinates; }

  // london_postcodes_dest [Postcode, Latitude, Longitude]
  public Coordinates getDropOffCoordinates() { return dropOffCoordinates; }
  public void setDropOffCoordinates(Coordinates dropOffCoordinates) { this.dropOffCoordinates = dropOffCoordinates; }

  // taxi_durations [pickup_datetime]
  public LocalDateTime getPickupDateTime() { return pickupDateTime; }
  public void setPickupDateTime(LocalDateTime pickupDateTime) { this.pickupDateTime = pickupDateTime; }

  // taxi_durations [dropoff_datetime]
  public LocalDateTime getDropOffDateTime() { return dropoffDateTime; }
  public void setDropOffDateTime(LocalDateTime dropoffDateTime) { this.dropoffDateTime = dropoffDateTime; }

  // random float generator in range 6.0...100.0
  public float getCost() { return cost; }
  public void setCost(float cost) { this.cost = cost; }

  // random float generator in range 0.0...5.0
  public int getDriverRating() { return driverRating; }
  public void setDriverRating(int driverRating) { this.driverRating = driverRating; }

  // driver_reviews [content]
  public String getDriverComment() { return driverComment; }
  public void setDriverComment(String driverComment) { this.driverComment = driverComment; }

  // random float generator in range 0.0...5.0
  public int getClientRating() { return clientRating; }
  public void setClientRating(int clientRating) { this.clientRating = clientRating; }

  // customer_reviews [reviews.title]
  public String getClientComment() { return clientComment; }
  public void setClientComment(String clientComment) { this.clientComment = clientComment; }
}
