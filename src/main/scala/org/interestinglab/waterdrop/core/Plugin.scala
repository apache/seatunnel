package org.interestinglab.waterdrop.core

abstract class Plugin extends Serializable {

    /**
     *  Return true and empty string if config is valid, return false and error message if config is invalid.
     */
    def checkConfig() : (Boolean, String)

    /**
      * Get Plugin Name.
      */
    def name : String = this.getClass.getName
}
