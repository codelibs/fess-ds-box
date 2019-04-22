/*
 * Copyright 2012-2019 CodeLibs Project and the Others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.codelibs.fess.ds.box

import org.codelibs.fess.util.ComponentUtil
import org.dbflute.utflute.lastaflute.LastaFluteTestCase


class BoxDataStoreTest : LastaFluteTestCase() {

    companion object {
        const val CLIENT_ID = ""
        const val CLIENT_SECRET = ""
        const val PUBLIC_KEY_ID = ""
        const val PRIVATE_KEY = ""
        const val PASSPHRASE = ""
        const val ENTERPRISE_ID = ""
    }

    private lateinit var dataStore: BoxDataStore

    override fun prepareConfigFile(): String = "test_app.xml"
    override fun isSuppressTestCaseTransaction(): Boolean = true

    override fun setUp() {
        super.setUp()
        dataStore = BoxDataStore()
    }

    override fun tearDown() {
        ComponentUtil.setFessConfig(null)
        super.tearDown()
    }

    fun testAPIConnection() {
        dataStore.getAPIConnection(PUBLIC_KEY_ID, PASSPHRASE, PRIVATE_KEY, CLIENT_ID, CLIENT_SECRET, ENTERPRISE_ID)
    }

}
