package seep.comm.serialization;

import java.util.ArrayList;

public class DataTuple {

	private int tupleSchemaId;
	private long timestamp;
	private int id;
	
	private Payload payload;
	
	// Required empty constructor for Kryo serialization
	public DataTuple(){
		
	}
	
	public int getTupleSchemaId(){
		return tupleSchemaId;
	}
	
	public long getTs() {
		return timestamp;
	}

	public void setTs(long timestamp) {
		this.timestamp = timestamp;
	}
	
	public void setId(int id){
		this.id = id;
	}
	
	public int getId(){
		return id;
	}
	
	public DataTuple(int tupleSchemaId){
		this.tupleSchemaId = tupleSchemaId;
	}
	
	public DataTuple(int tupleSchemaId, long timestamp, int id){
		this.tupleSchemaId = tupleSchemaId;
		this.timestamp = timestamp;
		this.id = id;
	}
	
	/*Ad-hoc for experimentation*/
	
	
	private int userId;
	private int itemId;
	private int rating;
	private ArrayList userVector;
	private ArrayList<Integer> partialRecommendationVector;
	private ArrayList noRec;
	private int recommendation;
	
	public int getUserId() {
		return userId;
	}

	public void setUserId(int userId) {
		this.userId = userId;
	}

	public int getItemId() {
		return itemId;
	}

	public void setItemId(int itemId) {
		this.itemId = itemId;
	}

	public int getRating() {
		return rating;
	}

	public void setRating(int rating) {
		this.rating = rating;
	}
	
	public ArrayList getUserVector(){
		return userVector;
	}
	
	public void setUserVector(ArrayList userVector){
		this.userVector = userVector;
	}
	
	public ArrayList<Integer> getPartialRecommendationVector(){
		return partialRecommendationVector;
	}
	
	public void setPartialRecommendationVector(ArrayList<Integer> partialRecommendationVector){
		this.partialRecommendationVector = partialRecommendationVector;
	}
	
	public ArrayList getNoRec(){
		return noRec;
	}
	
	public void setNoRec(ArrayList noRec){
		this.noRec = noRec;
	}
	
	public int getRecommendation(){
		return recommendation;
	}
	
	public void setRecommendation(int recommendation){
		this.recommendation = recommendation;
	}
	
//	private int second;
//	private double strikePrice;
//	private String exchangeId;
//	private int expiryDay;
//	private int expiryYear;
//	private String month;
//	private int monthCode;
//	
//	private int key;
//	
//	private String xParity;
//	
//	public String getxParity() {
//		return xParity;
//	}
//
//	public void setxParity(String xParity) {
//		this.xParity = xParity;
//	}
//
//	public int getMonthCode() {
//		return monthCode;
//	}
//
//	public void setMonthCode(int monthCode) {
//		this.monthCode = monthCode;
//	}
//
//	public long getTimestamp() {
//		return timestamp;
//	}
//
//	public void setTimestamp(long timestamp) {
//		this.timestamp = timestamp;
//	}
//
//	public int getSecond() {
//		return second;
//	}
//
//	public void setSecond(int second) {
//		this.second = second;
//	}
//
//	public double getStrikePrice() {
//		return strikePrice;
//	}
//
//	public void setStrikePrice(double d) {
//		this.strikePrice = d;
//	}
//
//	public String getExchangeId() {
//		return exchangeId;
//	}
//
//	public void setExchangeId(String exchangeId) {
//		this.exchangeId = exchangeId;
//	}
//
//	public int getExpiryDay() {
//		return expiryDay;
//	}
//
//	public void setExpiryDay(int expiryDay) {
//		this.expiryDay = expiryDay;
//	}
//
//	public int getExpiryYear() {
//		return expiryYear;
//	}
//
//	public void setExpiryYear(int expiryYear) {
//		this.expiryYear = expiryYear;
//	}
//
//	public String getMonth() {
//		return month;
//	}
//
//	public void setMonth(String month) {
//		this.month = month;
//	}
//
//	public void setKey(int key) {
//		this.key = key;
//	}
//
//	public int getKey() {
//		return key;
//	}
}
