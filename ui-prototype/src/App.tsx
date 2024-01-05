import React from 'react';
import logo from './logo.svg';
import './App.css';
import DownloadButton from './components/downloadButton';
import Link from './components/link';
import {SectionTitle1, SectionTitle2, SectionTitle4} from './components/sectionTitle';
import * as data from './data';
import HeaderSection from './components/headerSection';

function App() {

  return (
    <div className='grid grid-cols-[2fr_7fr_2fr]'>
    <div>
    </div>
    <div className=''>
      <h2><SectionTitle1>Pages:</SectionTitle1></h2>
      <ul className='pl-5'>
        <li><SectionTitle2>- Home</SectionTitle2></li>
        <li><SectionTitle2>- Help</SectionTitle2></li>
        <li>
          <SectionTitle4 className='py-2'>Site route menu </SectionTitle4>
          <ul className='pl-5'>
            <li>
            <SectionTitle2>- Datasets</SectionTitle2>
              <p className='pl-5'>
                Notes: <i> Could display date.
                          Could implement basic search.
                </i>
              </p>
              <p className='pl-5'>
                Components:
                <ul className='pl-5'>
                  <li><Link href={"#headerSection"}>- Title + Description header div</Link></li>
                  <li>- Did you know about the API box</li>
                  <li>- List of links to datasets and descriptons</li>
                </ul>
              </p>
            </li>
            <li>
            <SectionTitle2>- Dataset</SectionTitle2>
              <p className='pl-5'>
                Notes: <i> Could display human-readable date and time then timestamp
                10/06/1995 10:55 (2023-09-28T07:50:28.286977432Z). 
                Display empty
                about fields like Publisher if there is no data? 
                Display related datasets title if there are no related datasets?</i>
              </p>
              <p className='pl-5'>
                Components:
                <ul className='pl-5'>
                  <li><Link href={"#headerSection"}>- Title + Description header div</Link></li>
                  <li>- Did you know about the API box</li>
                  <li>- List of links to releases</li>
                  <li>- About dataset</li>
                </ul>
              </p>

            </li>
            <li><SectionTitle2>- Release</SectionTitle2>

            </li>
              <p className='pl-5'>
                  Components:
                  <ul className='pl-5'>
                    <li><Link href={"#headerSection"}>- Title + Description header div</Link></li>
                    <li><Link href={"#downloadButton"}>- Download Button</Link></li>
                    <li>- What changed drop down</li>
                    <li>- Did you know about the API box</li>
                    <li>- Link to revisions</li>
                    <li>- Table of schema columns</li>
                  </ul>
              </p>

            <li>
            <SectionTitle2>- Revisions</SectionTitle2>
              <p className='pl-5'>
                Notes: <i>Could contain download buttons instead of having a
                separate page for each revision?</i>
              </p>
              <p className='pl-5'>
                Components:
                <ul className='pl-5'>
                  <li><Link href={"#headerSection"}>- Title + Description header div</Link></li>
                  <li>- Table of revisions</li>
                </ul>
              </p>

            </li>
            <li><SectionTitle2>- Revision</SectionTitle2>

            </li>
              <p className='pl-5'>
                Components:
                <ul className='pl-5'>
                  <li><Link href={"#headerSection"}>- Title + Description header div</Link></li>
                  <li><Link href={"#downloadButton"}>- Download Button</Link></li>
                  <li>- What changed drop down</li>
                  <li>- Did you know about the API box</li>
                </ul>
              </p>
          </ul>
        </li>
      </ul>
    <div>
      <ul>
        <li>
          <SectionTitle1 className='py-10 text-center' id='headerSection'>Title + Description header</SectionTitle1>
          <i>Can choose to display date modified, created or both. If dates are the same, only shows the last modified.</i>
          <HeaderSection  className='py-10' title={data.dataset['dcterms:title']} 
          description={data.dataset['dcterms:description']} 
          dateCreated={data.dataset['dcterms:issued']}
          dateModified={data.dataset['dcterms:modified']}/>
        </li>

        <li>
          <SectionTitle1 className='py-10 text-center' id='downloadButton'>Download Button</SectionTitle1>
          <DownloadButton onClick={() => alert('Downloading CSV')}>
            Download dataset (CSV)
          </DownloadButton>
        </li>
        <li>
          <SectionTitle1 className='py-10 text-center'>What changed (drop down)</SectionTitle1>
        </li>
      </ul>
    </div>
    </div>
        <div>
      </div>
      </div>
  );
}

export default App;
