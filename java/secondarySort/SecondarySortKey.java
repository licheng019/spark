package com.chengli.spark.ex.sparkCore;

import java.io.Serializable;

import scala.math.Ordered;

public class SecondarySortKey implements Ordered<SecondarySortKey>, Serializable{

	private int first;
	private int second;
	@Override
	public boolean $greater(SecondarySortKey other) {
		if (this.first > other.getFirst()) {
			return true;
		} else if (this.first == other.getFirst() && this.second > other.getSecond()) {
			return true;
		} else {
			return false;
		}
	}

	@Override
	public boolean $greater$eq(SecondarySortKey other) {
		if (this.$greater(other)) {
			return true;
		} else if (this.first == other.getFirst() && this.second == other.getSecond()) {
			return true;
		}
		return false;
	}

	@Override
	public boolean $less(SecondarySortKey other) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean $less$eq(SecondarySortKey other) {
		if (this.first < other.getFirst()) {
			return true;
		} else if (this.first == other.getFirst() && this.second < other.getSecond()) {
			return true;
		} else {
			return false;
		}
	}

	@Override
	public int compare(SecondarySortKey other) {
		
		if (this.first - other.getFirst() != 0) {
			return this.first - other.getFirst();
		} else {
			return this.second - other.getSecond();
		}
	}

	@Override
	public int compareTo(SecondarySortKey other) {
		// TODO Auto-generated method stub
		if (this.first - other.getFirst() != 0) {
			return this.first - other.getFirst();
		} else {
			return this.second - other.getSecond();
		}
	}

	public int getFirst() {
		return first;
	}

	public void setFirst(int first) {
		this.first = first;
	}

	public int getSecond() {
		return second;
	}

	public void setSecond(int second) {
		this.second = second;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + first;
		result = prime * result + second;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SecondarySortKey other = (SecondarySortKey) obj;
		if (first != other.first)
			return false;
		if (second != other.second)
			return false;
		return true;
	}
	
}
