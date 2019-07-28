package com.yuyu.stream.spark.key;

import scala.math.Ordered;

import java.io.Serializable;

/**
 * @author yuyu
 */
public class UserHourSystemKey implements Ordered<UserHourSystemKey>, Serializable {
    public Long userId;
    public String hour;
    public String systemName;

    @Override
    public int compareTo(UserHourSystemKey that) {
        if (this.userId.equals(getUserId())) {
            if (this.hour.compareTo(that.getHour())==0) {
                return this.systemName.compareTo(that.getsystemName());
            } else {
                return this.hour.compareTo(that.getHour());
            }
        } else {
            Long n = this.userId - that.getUserId();
            return n > 0 ? 1 : (n == 0 ? 0 : -1);
        }
    }

    @Override
    public int compare(UserHourSystemKey that)
    {
        return this.compareTo(that);
    }

    @Override
    public boolean $greater(UserHourSystemKey that)
    {
        if (this.compareTo(that) > 0) {
            return true;
        }

        return false;
    }

    @Override
    public boolean $less(UserHourSystemKey that)
    {
        if (this.compareTo(that) < 0) {
            return true;
        }

        return false;
    }

    @Override
    public boolean $less$eq(UserHourSystemKey that)
    {
        if (this.compareTo(that) <= 0) {
            return true;
        }

        return false;
    }

    @Override
    public boolean $greater$eq(UserHourSystemKey that) {
        if (this.compareTo(that) >= 0) {
            return true;
        }

        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        UserHourSystemKey that = (UserHourSystemKey) o;

        if (userId != null ? !userId.equals(that.userId) : that.userId != null) {
            return false;
        }
        if (hour != null ? !hour.equals(that.hour) : that.hour != null) {
            return false;
        }
        return !(systemName != null ? !systemName.equals(that.systemName) : that.systemName != null);

    }

    @Override
    public int hashCode() {
        int result = userId != null ? userId.hashCode() : 0;
        result = 31 * result + (hour != null ? hour.hashCode() : 0);
        result = 31 * result + (systemName != null ? systemName.hashCode() : 0);
        return result;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getHour() {
        return hour;
    }

    public void setHour(String hour) {
        this.hour = hour;
    }

    public String getsystemName() {
        return systemName;
    }

    public void setsystemName(String systemName) {
        this.systemName = systemName;
    }

    @Override
    public String toString() {
        return "UserHourSystemKey{" +
                "userId=" + userId +
                ", hour='" + hour + '\'' +
                ", systemName='" + systemName + '\'' +
                '}';
    }
}
