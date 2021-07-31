package iti.Mahmoud;

import java.io.Serializable;
//@XmlRootElement(name ="user")
public class POJO  implements Serializable {
    private String title,company,location,type,level,yearExp,country,skills;

    public POJO(){}

    public String getTitle() {
        return title;
    }
//    @XmlElement
    public void setTitle(String title) {
        this.title = title;
    }

    public String getCompany() {
        return company;
    }

    public void setCompany(String company) {
        this.company = company;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public String getYearExp() {
        return yearExp;
    }

    public void setYearExp(String yearExp) {
        this.yearExp = yearExp;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getSkills() {
        return skills;
    }

    public void setSkills(String skills) {
        this.skills = skills;
    }

    @Override
    public String toString() {
        return "POJO{" +
                "title='" + title + '\'' +
                ", company='" + company + '\'' +
                ", location='" + location + '\'' +
                ", type='" + type + '\'' +
                ", level='" + level + '\'' +
                ", yearExp='" + yearExp + '\'' +
                ", country='" + country + '\'' +
                ", skills='" + skills + '\'' +
                '}';
    }

}
